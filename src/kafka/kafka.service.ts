import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import { getRetryDelay } from '../config/retry-policy';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  // Kafka client used to communicate with our Kafka broker.
  private readonly kafka: Kafka;

  // Producer is responsible for publishing messages to Kafka topics.
  private readonly producer: Producer;

  constructor() {
    this.kafka = new Kafka({
      // Identifies this application when communicating with Kafka.
      clientId: 'kafka-retry-framework',

      // Kafka broker exposed on localhost by Docker.
      brokers: ['localhost:9092'],
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
    // Get the delay associated with this retry attempt.
    const retryDelay = getRetryDelay(retryCount);

    // Calculate the time when this message should become eligible for retry.
    const scheduledRetryAt = new Date(Date.now() + retryDelay).toISOString();

    await this.producer.send({
      // Failed messages are published here for retry processing.
      topic: retryTopic,
      messages: [
        {
          // Original business payload.
          value,

          // Metadata used by our retry framework.
          // This is kept in Kafka headers instead of changing
          // the actual business payload.
          headers: {
            // Number of times this message has been retried.
            retry_count: retryCount.toString(),
            max_retries: '3',
            // Topic where the message originally came from.
            original_topic: 'orders',
            scheduled_retry_at: scheduledRetryAt,
          },
        },
      ],
    });
  }

  /**
   * Send a message to the Dead Letter Queue (DLQ)
   * after all retry attempts have been exhausted.
   */
  async sendToDlq(value: string, retryCount: number, errorMessage: string) {
    await this.producer.send({
      // Messages that cannot be successfully processed
      // after the maximum retries end up here.
      topic: 'orders.dlq',

      messages: [
        {
          value,

          // Store useful debugging information with the failed message.
          headers: {
            retry_count: retryCount.toString(),
            original_topic: 'orders',
            error_message: errorMessage,
          },
        },
      ],
    });
  }
}
