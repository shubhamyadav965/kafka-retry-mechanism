import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import { Consumer, Kafka } from 'kafkajs';

import { KafkaService } from './kafka.service';
import { OrderProcessor } from '../orders/order.processor';
import { getRetryTopic } from '../config/retry-policy';

@Injectable()
export class KafkaConsumer implements OnModuleInit, OnModuleDestroy {
  // Kafka client used by this consumer.
  private readonly kafka: Kafka;

  // Consumer reads messages from all configured original topics.
  private readonly consumer: Consumer;

  constructor(
    private readonly kafkaService: KafkaService,
    private readonly orderProcessor: OrderProcessor,
    private readonly configService: ConfigService,
  ) {
    this.kafka = new Kafka({
      // Identifier for this Kafka client.
      clientId: 'order-consumer',

      // Kafka broker address.
      brokers: ['kafka:9093'],
    });

    this.consumer = this.kafka.consumer({
      // Consumers with the same groupId share the work
      // and maintain a shared committed offset.
      groupId: 'order-consumer-group',
    });
  }

  async onModuleInit() {
    // Connect the consumer to Kafka.
    await this.consumer.connect();

    // Which original topics this consumer should handle, per config.
    const originalTopics =
      this.configService
        .get<string>('KAFKA_ORIGINAL_TOPICS')
        ?.split(',')
        .map((topic) => topic.trim())
        .filter(Boolean) ?? [];

    // Subscribe this consumer to every configured original topic.
    await this.consumer.subscribe({
      topics: originalTopics,

      // Start from the beginning when this consumer group
      // does not already have a committed offset.
      fromBeginning: true,
    });

    await this.consumer.run({
      // eachMessage is called whenever Kafka delivers
      // a message to this consumer.
      eachMessage: async ({ topic, message }) => {
        // Kafka message values arrive as Buffers.
        // Convert the value into a string.
        const value = message.value?.toString();

        console.log(`Received message on ${topic}:`, value);

        try {
          // Business logic is kept outside the consumer.
          // This makes the same processor reusable by
          // both the main and retry consumers.
          await this.orderProcessor.process(value ?? '');
        } catch {
          console.log('Processing failed. Sending to retry topic...');

          // First failure from the main topic becomes retry #1.
          const retryCount = 1;

          await this.kafkaService.sendToRetryTopic(
            value ?? '',
            retryCount,
            getRetryTopic(this.configService, topic, retryCount),
            topic,
          );
        }
      },
    });

    console.log('Kafka consumer connected');
  }

  async onModuleDestroy() {
    // Gracefully leave the consumer group and close
    // the Kafka connection when the application shuts down.
    await this.consumer.disconnect();

    console.log('Kafka consumer disconnected');
  }
}
