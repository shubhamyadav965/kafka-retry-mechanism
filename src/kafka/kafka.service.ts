import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Producer } from 'kafkajs';
import {
  getRetryDelay,
  getMaxRetries,
  getDlqTopic,
} from '../config/retry-policy';
import { RetryJob } from '../redis/retry-job';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  // Kafka client used to communicate with our Kafka broker.
  private readonly kafka: Kafka;

  // Producer is responsible for publishing messages to Kafka topics.
  private readonly producer: Producer;

  constructor(private readonly configService: ConfigService) {
    const brokers =
      this.configService
        .get<string>('KAFKA_BROKERS')
        ?.split(',')
        .map((broker) => broker.trim()) ?? [];

    this.kafka = new Kafka({
      // Identifies this application when communicating with Kafka.
      clientId: 'kafka-retry-framework',

      brokers,
    });

    // Create the Kafka producer.
    this.producer = this.kafka.producer();
  }

  async onModuleInit() {
    // Establish connection with Kafka when NestJS starts.
    await this.producer.connect();

    console.log('Kafka producer connected');
  }

  async onModuleDestroy() {
    // Gracefully close the Kafka connection when NestJS shuts down.
    await this.producer.disconnect();

    console.log('Kafka producer disconnected');
  }

  /**
   * Publish a message to an arbitrary topic.
   *
   * Used by the API layer (e.g. POST /orders) to publish new
   * business events onto the main orders topic.
   */
  async sendMessage(topic: string, message: unknown) {
    await this.producer.send({
      topic,
      messages: [
        {
          // Kafka payload is bytes, not objects.
          value: JSON.stringify(message),
        },
      ],
    });
  }

  /**
   * Publish a failed message to the retry topic.
   *
   * We don't physically move the original Kafka message.
   * Instead, we create a new Kafka message in orders.retry.
   */
  async sendToRetryTopic(
    value: string,
    retryCount: number,
    retryTopic: string,
  ) {
    const retryDelay = getRetryDelay(retryCount);
    const scheduledRetryAt = new Date(Date.now() + retryDelay).toISOString();

    await this.producer.send({
      topic: retryTopic,
      messages: [
        {
          value,
          headers: {
            retry_count: retryCount.toString(),
            max_retries: getMaxRetries().toString(),
            original_topic: 'orders',
            scheduled_retry_at: scheduledRetryAt,
          },
        },
      ],
    });
  }

  //Publish a scheduled retry job back to Kafka. The RetryScheduler calls this method when the retry becomes due in Redis.
  async publishRetryJob(job: RetryJob): Promise<void> {
    await this.producer.send({
      topic: job.retryTopic,
      messages: [
        {
          // Send the original business message unchanged.
          value: job.value,
          // Restore retry metadata as Kafka headers.
          headers: {
            retry_count: job.retryCount.toString(),
            max_retries: getMaxRetries().toString(),
            original_topic: job.originalTopic,
            scheduled_retry_at: job.scheduledRetryAt,
          },
        },
      ],
    });
  }

  /**
   * Send a message to the Dead Letter Queue (DLQ)
   * after all retry attempts have been exhausted.
   */
  async sendToDlq(
    value: string,
    retryCount: number,
    errorMessage: string,
    originalTopic: string,
  ) {
    await this.producer.send({
      topic: getDlqTopic(originalTopic),
      messages: [
        {
          value,
          headers: {
            retry_count: retryCount.toString(),
            original_topic: originalTopic,
            error_message: errorMessage,
          },
        },
      ],
    });
  }
}
