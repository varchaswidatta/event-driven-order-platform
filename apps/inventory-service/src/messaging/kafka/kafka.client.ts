import { Kafka, KafkaConfig, logLevel } from 'kafkajs';
import { env } from '../../config/env.js';

export interface CreateKafkaClientOptions {
  clientId?: string;
  brokers?: string[];
  logLevel?: logLevel;
}

/**
 * Creates a configured KafkaJS client instance using centralized application configuration.
 */
export function createKafkaClient(options?: CreateKafkaClientOptions): Kafka {
  const brokers = options?.brokers ?? env.KAFKA_BROKERS.split(',').map((b) => b.trim());
  const clientId = options?.clientId ?? env.KAFKA_CLIENT_ID;

  const config: KafkaConfig = {
    clientId,
    brokers,
    logLevel: options?.logLevel ?? logLevel.NOTHING,
    retry: {
      initialRetryTime: 100,
      retries: 5,
    },
  };

  return new Kafka(config);
}
