import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import { Kafka, Producer } from 'kafkajs';
import crypto from 'node:crypto';
import { createDatabasePool as createStockDbPool } from '../../../stock-service/src/db/client.js';
import { runMigrations as runStockMigrations } from '../../../stock-service/src/db/migrate.js';
import { StockRepository } from '../../../stock-service/src/repositories/stock.repository.js';
import { startGrpcServer, RunningGrpcServer } from '../../../stock-service/src/grpc/server.js';
import { createDatabasePool as createInventoryDbPool } from '../../src/db/client.js';
import { runMigrations as runInventoryMigrations } from '../../src/db/migrate.js';
import { InventoryRepository } from '../../src/repositories/inventory.repository.js';
import { InventoryService } from '../../src/services/inventory.service.js';
import { StockServiceClient } from '../../src/clients/stock-service.client.js';
import {
  OrderEventsConsumer,
  ORDER_EVENTS_TOPIC,
} from '../../src/messaging/kafka/order-events.consumer.js';
import { env } from '../../src/config/env.js';
import { RESERVATION_STATUS } from '../../src/domain/reservation-status.js';

describe('Inventory -> Stock Service Integration Tests (Kafka -> Inventory -> gRPC -> Stock -> stock_db)', () => {
  let stockPool: Pool;
  let stockRepository: StockRepository;
  let grpcServer: RunningGrpcServer;
  let stockClient: StockServiceClient;

  let inventoryPool: Pool;
  let inventoryRepository: InventoryRepository;
  let inventoryService: InventoryService;
  let consumer: OrderEventsConsumer;

  let kafka: Kafka;
  let testProducer: Producer;

  beforeAll(async () => {
    // 1. Stock database and gRPC server setup
    stockPool = createStockDbPool({ max: 10 });
    await runStockMigrations(stockPool);
    stockRepository = new StockRepository(stockPool);

    grpcServer = await startGrpcServer({
      port: 0,
      host: '127.0.0.1',
      pool: stockPool,
    });

    stockClient = new StockServiceClient({
      host: '127.0.0.1',
      port: grpcServer.port,
      timeoutMs: 5000,
    });

    // 2. Inventory database setup
    inventoryPool = createInventoryDbPool({ max: 5 });
    await runInventoryMigrations(inventoryPool);
    inventoryRepository = new InventoryRepository(inventoryPool);

    // 3. Inventory Service wired with real gRPC StockServiceClient
    inventoryService = new InventoryService(inventoryRepository, stockClient);

    // 4. Kafka setup
    kafka = new Kafka({
      clientId: 'inventory-stock-test-client',
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
    });

    testProducer = kafka.producer();
    await testProducer.connect();

    consumer = new OrderEventsConsumer(kafka, inventoryService, {
      groupId: `inventory-stock-test-group-${crypto.randomUUID()}`,
      topic: ORDER_EVENTS_TOPIC,
    });
    await consumer.start();

    // Stabilization pause for Kafka partition assignment
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }, 40000);

  beforeEach(async () => {
    await inventoryPool.query('DELETE FROM inventory_reservation_items;');
    await inventoryPool.query('DELETE FROM inventory_reservations;');
    await stockPool.query('DELETE FROM stock_reservation_items;');
    await stockPool.query('DELETE FROM stock_reservations;');
    await stockPool.query('DELETE FROM stock;');
    await stockPool.query('DELETE FROM products;');
  });

  afterAll(async () => {
    if (consumer) {
      await consumer.stop().catch(() => {});
    }
    if (testProducer) {
      await testProducer.disconnect().catch(() => {});
    }
    if (stockClient) {
      stockClient.close();
    }
    if (grpcServer) {
      await grpcServer.close().catch(() => {});
    }
    if (inventoryPool) {
      await inventoryPool.query('DELETE FROM inventory_reservation_items;').catch(() => {});
      await inventoryPool.query('DELETE FROM inventory_reservations;').catch(() => {});
      await inventoryPool.end().catch(() => {});
    }
    if (stockPool) {
      await stockPool.query('DELETE FROM stock_reservation_items;').catch(() => {});
      await stockPool.query('DELETE FROM stock_reservations;').catch(() => {});
      await stockPool.query('DELETE FROM stock;').catch(() => {});
      await stockPool.query('DELETE FROM products;').catch(() => {});
      await stockPool.end().catch(() => {});
    }
  });

  it('Requirement 15.E: Kafka OrderCreated -> Inventory Service -> gRPC ReserveStock -> stock_db stock reservation', async () => {
    // Seed product in stock_db
    const { product } = await stockRepository.createProductWithStock({
      sku: 'SKU-INTEG-1',
      name: 'Integration Test Product',
      price: '49.99',
      availableQuantity: 50,
    });

    const orderId = crypto.randomUUID();
    const customerId = crypto.randomUUID();

    const orderCreatedMessage = {
      eventId: crypto.randomUUID(),
      eventType: 'OrderCreated',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'Order',
      aggregateId: orderId,
      correlationId: crypto.randomUUID(),
      payload: {
        orderId,
        customerId,
        items: [{ productId: product.id, quantity: 10 }],
      },
    };

    // Publish to Kafka topic order.events
    await testProducer.send({
      topic: ORDER_EVENTS_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify(orderCreatedMessage),
        },
      ],
    });

    // Wait until inventory_db has the reservation
    let inventoryReservation = null;
    let attempts = 0;
    while (!inventoryReservation && attempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 200));
      inventoryReservation = await inventoryRepository.findReservationByOrderId(orderId);
      attempts++;
    }

    expect(inventoryReservation).not.toBeNull();
    expect(inventoryReservation!.orderId).toBe(orderId);
    expect(inventoryReservation!.status).toBe(RESERVATION_STATUS.PENDING);

    // Wait until stock_db reflects the gRPC reservation
    let stockReservation = null;
    let stockAttempts = 0;
    while (!stockReservation && stockAttempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 200));
      stockReservation = await stockRepository.getReservationByOrderId(orderId);
      stockAttempts++;
    }

    // Verify stock reservation in stock_db
    expect(stockReservation).not.toBeNull();
    expect(stockReservation!.orderId).toBe(orderId);
    expect(stockReservation!.status).toBe('RESERVED');
    expect(stockReservation!.items).toHaveLength(1);
    expect(stockReservation!.items![0]!.productId).toBe(product.id);
    expect(stockReservation!.items![0]!.quantity).toBe(10);

    // Verify stock table mutation: available decremented by 10, reserved incremented by 10
    const updatedStock = await stockRepository.getStock(product.id);
    expect(updatedStock!.availableQuantity).toBe(40);
    expect(updatedStock!.reservedQuantity).toBe(10);
  }, 30000);

  it('Requirement 15.F: Duplicate delivery test: stock is NOT reserved twice when same OrderCreated event is delivered twice', async () => {
    // Seed product with initial available = 30
    const { product } = await stockRepository.createProductWithStock({
      sku: 'SKU-IDEMPOTENT-2',
      name: 'Idempotency Test Product',
      price: '99.99',
      availableQuantity: 30,
    });

    const orderId = crypto.randomUUID();
    const customerId = crypto.randomUUID();

    const orderCreatedMessage = {
      eventId: crypto.randomUUID(),
      eventType: 'OrderCreated',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'Order',
      aggregateId: orderId,
      correlationId: crypto.randomUUID(),
      payload: {
        orderId,
        customerId,
        items: [{ productId: product.id, quantity: 5 }],
      },
    };

    // First delivery
    await testProducer.send({
      topic: ORDER_EVENTS_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify(orderCreatedMessage),
        },
      ],
    });

    // Wait for first delivery to complete
    let stockReservation = null;
    let attempts = 0;
    while (!stockReservation && attempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 200));
      stockReservation = await stockRepository.getReservationByOrderId(orderId);
      attempts++;
    }

    expect(stockReservation).not.toBeNull();
    const originalReservationId = stockReservation!.id;

    // Verify stock after first delivery: available = 25, reserved = 5
    const stockAfterFirst = await stockRepository.getStock(product.id);
    expect(stockAfterFirst!.availableQuantity).toBe(25);
    expect(stockAfterFirst!.reservedQuantity).toBe(5);

    // Second duplicate delivery with identical orderId and payload
    await testProducer.send({
      topic: ORDER_EVENTS_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify(orderCreatedMessage),
        },
      ],
    });

    // Wait a brief period to ensure consumer processes the duplicate
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // CRITICAL ASSERTION: Stock MUST NOT be decremented again!
    const stockAfterDuplicate = await stockRepository.getStock(product.id);
    expect(stockAfterDuplicate!.availableQuantity).toBe(25); // Still 25, NOT 20
    expect(stockAfterDuplicate!.reservedQuantity).toBe(5); // Still 5, NOT 10

    // Verify only 1 reservation row exists in inventory_db
    const invCount = await inventoryPool.query(
      'SELECT COUNT(*)::int as count FROM inventory_reservations WHERE order_id = $1;',
      [orderId],
    );
    expect(invCount.rows[0].count).toBe(1);

    // Verify only 1 reservation row exists in stock_db
    const stockCount = await stockPool.query(
      'SELECT COUNT(*)::int as count FROM stock_reservations WHERE order_id = $1;',
      [orderId],
    );
    expect(stockCount.rows[0].count).toBe(1);

    // Reservation ID remains identical
    const currentStockRes = await stockRepository.getReservationByOrderId(orderId);
    expect(currentStockRes!.id).toBe(originalReservationId);
  }, 35000);

  it('handles business failure: insufficient stock does not mutate stock_db when requested quantity exceeds available', async () => {
    // Seed product with available = 2
    const { product } = await stockRepository.createProductWithStock({
      sku: 'SKU-INSUFF-1',
      name: 'Insufficient Product',
      price: '12.00',
      availableQuantity: 2,
    });

    const orderId = crypto.randomUUID();
    const customerId = crypto.randomUUID();

    const orderCreatedMessage = {
      eventId: crypto.randomUUID(),
      eventType: 'OrderCreated',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'Order',
      aggregateId: orderId,
      correlationId: crypto.randomUUID(),
      payload: {
        orderId,
        customerId,
        items: [{ productId: product.id, quantity: 10 }], // Exceeds available 2
      },
    };

    await testProducer.send({
      topic: ORDER_EVENTS_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify(orderCreatedMessage),
        },
      ],
    });

    // Wait until inventory reservation is created
    let invRes = null;
    let attempts = 0;
    while (!invRes && attempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 200));
      invRes = await inventoryRepository.findReservationByOrderId(orderId);
      attempts++;
    }

    expect(invRes).not.toBeNull();
    expect(invRes!.orderId).toBe(orderId);

    // Brief wait for gRPC processing
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Verify stock_db: stock was NOT mutated
    const stock = await stockRepository.getStock(product.id);
    expect(stock!.availableQuantity).toBe(2);
    expect(stock!.reservedQuantity).toBe(0);

    // Verify no reservation created in stock_db
    const stockRes = await stockRepository.getReservationByOrderId(orderId);
    expect(stockRes).toBeNull();
  }, 30000);
});
