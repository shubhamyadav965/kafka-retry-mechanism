import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';

import { Consumer, Kafka } from 'kafkajs';

import { KafkaService } from './kafka.service';
import { OrderProcessor } from '../orders/order.processor';

@Injectable()
export class KafkaConsumer implements OnModuleInit, OnModuleDestroy {
  // Kafka client used by this consumer.
  private readonly kafka: Kafka;

  // Consumer reads messages from the orders topic.
  private readonly consumer: Consumer;

  constructor(
    private readonly kafkaService: KafkaService,
    private readonly orderProcessor: OrderProcessor,
  ) {
    this.kafka = new Kafka({
      // Identifier for this Kafka client.
      clientId: 'order-consumer',

      // Kafka broker address.
      brokers: ['localhost:9092'],
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

    // Subscribe this consumer to the main orders topic.
    await this.consumer.subscribe({
      topic: 'orders',

      // Start from the beginning when this consumer group
      // does not already have a committed offset.
      fromBeginning: true,
    });

    await this.consumer.run({
      // eachMessage is called whenever Kafka delivers
      // a message to this consumer.
      eachMessage: async ({ message }) => {
        // Kafka message values arrive as Buffers.
        // Convert the value into a string.
        const value = message.value?.toString();

        console.log('Received message:', value);

        try {
          // Business logic is kept outside the consumer.
          // This makes the same processor reusable by
          // both the main and retry consumers.
          await this.orderProcessor.process(value ?? '');
        } catch {
          console.log('Processing failed. Sending to retry topic...');

          // First failure from the main topic becomes retry #1.
          await this.kafkaService.sendToRetryTopic(value ?? '', 1);
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
