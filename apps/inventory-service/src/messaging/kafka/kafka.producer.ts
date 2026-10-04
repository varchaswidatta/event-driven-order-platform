import { Producer, Kafka, Partitioners } from 'kafkajs';
import { createKafkaClient } from './kafka.client.js';

export interface PublishMessageOptions {
  topic: string;
  key: string;
  value: string;
  headers?: Record<string, string>;
}

export interface IKafkaProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  publish(options: PublishMessageOptions): Promise<void>;
  isConnected(): boolean;
}

/**
 * Dedicated Kafka producer for the Inventory Service.
 * Publishes inventory result events (InventoryReserved, InventoryReservationFailed)
 * to the inventory.events topic via the transactional outbox pattern.
 */
export class KafkaInventoryProducer implements IKafkaProducer {
  private readonly producer: Producer;
  private connected = false;

  constructor(kafka?: Kafka) {
    const client = kafka ?? createKafkaClient();
    this.producer = client.producer({
      allowAutoTopicCreation: true,
      transactionTimeout: 30000,
      createPartitioner: Partitioners.DefaultPartitioner,
    });
  }

  async connect(): Promise<void> {
    if (!this.connected) {
      await this.producer.connect();
      this.connected = true;
    }
  }

  async disconnect(): Promise<void> {
    if (this.connected) {
      await this.producer.disconnect();
      this.connected = false;
    }
  }

  async publish(options: PublishMessageOptions): Promise<void> {
    if (!this.connected) {
      await this.connect();
    }

    await this.producer.send({
      topic: options.topic,
      messages: [
        {
          key: options.key,
          value: options.value,
          headers: options.headers,
        },
      ],
    });
  }

  isConnected(): boolean {
    return this.connected;
  }
}
