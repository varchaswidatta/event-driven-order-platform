import { describe, it, expect, vi, beforeEach } from 'vitest';
import { OrderService } from '../../src/services/order.service.js';
import { IOrderRepository } from '../../src/repositories/order.repository.js';
import { InvalidOrderInputError, OrderNotFoundError } from '../../src/errors/order.errors.js';
import { ORDER_STATUS } from '../../src/domain/order-status.js';
import { Order } from '../../src/domain/order.js';

describe('OrderService (Unit Tests)', () => {
  let mockOrderRepository: IOrderRepository;
  let orderService: OrderService;

  const validCustomerId = '9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d';
  const validProductId1 = '123e4567-e89b-12d3-a456-426614174000';
  const validProductId2 = '223e4567-e89b-12d3-a456-426614174001';

  beforeEach(() => {
    mockOrderRepository = {
      createOrder: vi.fn(),
      findOrderById: vi.fn(),
      findOrdersByCustomerId: vi.fn(),
    };
    orderService = new OrderService(mockOrderRepository);
  });

  // 1. Valid order creation
  it('creates an order successfully with valid input', async () => {
    const input = {
      customerId: validCustomerId,
      currency: 'USD',
      items: [
        { productId: validProductId1, quantity: 2, unitPrice: '15.50' },
        { productId: validProductId2, quantity: 1, unitPrice: '9.00' },
      ],
    };

    const mockCreatedOrder: Order = {
      id: 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
      customerId: input.customerId,
      status: ORDER_STATUS.PENDING,
      totalAmount: '40.00',
      currency: 'USD',
      items: [
        {
          id: 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12',
          orderId: 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
          productId: validProductId1,
          quantity: 2,
          unitPrice: '15.50',
          createdAt: new Date(),
        },
        {
          id: 'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13',
          orderId: 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
          productId: validProductId2,
          quantity: 1,
          unitPrice: '9.00',
          createdAt: new Date(),
        },
      ],
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    vi.mocked(mockOrderRepository.createOrder).mockResolvedValue(mockCreatedOrder);

    const result = await orderService.createOrder(input);

    expect(result).toEqual(mockCreatedOrder);
    expect(mockOrderRepository.createOrder).toHaveBeenCalledTimes(1);
  });

  // 2. Empty items rejected
  it('rejects order with empty items array', async () => {
    const input = {
      customerId: validCustomerId,
      currency: 'USD',
      items: [],
    };

    await expect(orderService.createOrder(input)).rejects.toThrow(InvalidOrderInputError);
    await expect(orderService.createOrder(input)).rejects.toThrow(/Order items must not be empty/i);
    expect(mockOrderRepository.createOrder).not.toHaveBeenCalled();
  });

  // 3. Invalid quantity rejected (0, negative, floating point)
  it('rejects order with invalid quantity (zero, negative, or non-integer)', async () => {
    const inputWithZero = {
      customerId: validCustomerId,
      currency: 'USD',
      items: [{ productId: validProductId1, quantity: 0, unitPrice: '10.00' }],
    };
    await expect(orderService.createOrder(inputWithZero)).rejects.toThrow(InvalidOrderInputError);

    const inputWithNegative = {
      customerId: validCustomerId,
      currency: 'USD',
      items: [{ productId: validProductId1, quantity: -2, unitPrice: '10.00' }],
    };
    await expect(orderService.createOrder(inputWithNegative)).rejects.toThrow(
      InvalidOrderInputError,
    );

    const inputWithFloat = {
      customerId: validCustomerId,
      currency: 'USD',
      items: [{ productId: validProductId1, quantity: 1.5, unitPrice: '10.00' }],
    };
    await expect(orderService.createOrder(inputWithFloat)).rejects.toThrow(InvalidOrderInputError);

    expect(mockOrderRepository.createOrder).not.toHaveBeenCalled();
  });

  // 4. Invalid product ID rejected
  it('rejects order with non-UUID product ID', async () => {
    const input = {
      customerId: validCustomerId,
      currency: 'USD',
      items: [{ productId: 'not-a-valid-uuid', quantity: 1, unitPrice: '10.00' }],
    };

    await expect(orderService.createOrder(input)).rejects.toThrow(InvalidOrderInputError);
    await expect(orderService.createOrder(input)).rejects.toThrow(
      /Product ID must be a valid UUID/i,
    );
    expect(mockOrderRepository.createOrder).not.toHaveBeenCalled();
  });

  // 5. Invalid customer ID rejected
  it('rejects order with non-UUID customer ID', async () => {
    const input = {
      customerId: 'invalid-customer-123',
      currency: 'USD',
      items: [{ productId: validProductId1, quantity: 1, unitPrice: '10.00' }],
    };

    await expect(orderService.createOrder(input)).rejects.toThrow(InvalidOrderInputError);
    await expect(orderService.createOrder(input)).rejects.toThrow(
      /Customer ID must be a valid UUID/i,
    );
    expect(mockOrderRepository.createOrder).not.toHaveBeenCalled();
  });

  // 6. Negative unit price rejected
  it('rejects order with negative unit price', async () => {
    const input = {
      customerId: validCustomerId,
      currency: 'USD',
      items: [{ productId: validProductId1, quantity: 1, unitPrice: '-5.00' }],
    };

    await expect(orderService.createOrder(input)).rejects.toThrow(InvalidOrderInputError);
    await expect(orderService.createOrder(input)).rejects.toThrow(
      /Unit price must be a valid non-negative decimal/i,
    );
    expect(mockOrderRepository.createOrder).not.toHaveBeenCalled();
  });

  // 7. Correct total amount calculation
  it('calculates total amount accurately using decimal-safe arithmetic', async () => {
    const input = {
      customerId: validCustomerId,
      currency: 'USD',
      items: [
        { productId: validProductId1, quantity: 3, unitPrice: '19.99' }, // 59.97
        { productId: validProductId2, quantity: 2, unitPrice: '10.01' }, // 20.02
      ], // total: 79.99
    };

    vi.mocked(mockOrderRepository.createOrder).mockImplementation(async (data) => ({
      id: 'test-order-id',
      customerId: data.customerId,
      status: data.status,
      totalAmount: data.totalAmount,
      currency: data.currency,
      items: [],
      createdAt: new Date(),
      updatedAt: new Date(),
    }));

    const result = await orderService.createOrder(input);

    expect(result.totalAmount).toBe('79.99');
    expect(mockOrderRepository.createOrder).toHaveBeenCalledWith(
      expect.objectContaining({
        totalAmount: '79.99',
      }),
    );
  });

  // 8. Initial status is PENDING
  it('sets initial order status to PENDING', async () => {
    const input = {
      customerId: validCustomerId,
      currency: 'EUR',
      items: [{ productId: validProductId1, quantity: 1, unitPrice: '25.00' }],
    };

    vi.mocked(mockOrderRepository.createOrder).mockImplementation(async (data) => ({
      id: 'test-order-id',
      customerId: data.customerId,
      status: data.status,
      totalAmount: data.totalAmount,
      currency: data.currency,
      items: [],
      createdAt: new Date(),
      updatedAt: new Date(),
    }));

    const result = await orderService.createOrder(input);

    expect(result.status).toBe(ORDER_STATUS.PENDING);
    expect(mockOrderRepository.createOrder).toHaveBeenCalledWith(
      expect.objectContaining({
        status: ORDER_STATUS.PENDING,
      }),
    );
  });

  // 9. Repository/service interaction behaves correctly
  it('properly passes mapped order data to repository and handles query retrieval', async () => {
    const orderId = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';
    const mockOrder: Order = {
      id: orderId,
      customerId: validCustomerId,
      status: ORDER_STATUS.PENDING,
      totalAmount: '50.00',
      currency: 'USD',
      items: [],
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    vi.mocked(mockOrderRepository.findOrderById).mockResolvedValue(mockOrder);

    const fetched = await orderService.getOrderById(orderId);
    expect(fetched).toEqual(mockOrder);
    expect(mockOrderRepository.findOrderById).toHaveBeenCalledWith(orderId);

    // Test order not found
    vi.mocked(mockOrderRepository.findOrderById).mockResolvedValue(null);
    await expect(orderService.getOrderById('non-existent-id')).rejects.toThrow(OrderNotFoundError);

    // Test find orders by customer
    vi.mocked(mockOrderRepository.findOrdersByCustomerId).mockResolvedValue([mockOrder]);
    const customerOrders = await orderService.getOrdersByCustomerId(validCustomerId);
    expect(customerOrders).toEqual([mockOrder]);
    expect(mockOrderRepository.findOrdersByCustomerId).toHaveBeenCalledWith(validCustomerId);
  });
});
