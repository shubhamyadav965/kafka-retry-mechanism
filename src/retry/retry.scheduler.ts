import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';

import { Consumer, Kafka } from 'kafkajs';

import { OrderProcessor } from '../orders/order.processor';
import { KafkaService } from '../kafka/kafka.service';

@Injectable()
export class RetryScheduler implements OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

  constructor(
    private readonly orderProcessor: OrderProcessor,
    private readonly kafkaService: KafkaService,
  ) {
    this.kafka = new Kafka({
      clientId: 'retry-scheduler',
      brokers: ['kafka:9093'],
    });

    this.consumer = this.kafka.consumer({
      groupId: 'retry-scheduler-group',
    });
  }

  async onModuleInit() {
    await this.consumer.connect();

    // The scheduler listens to all retry-stage topics.
    await this.consumer.subscribe({
      topic: 'orders.retry.1m',
      fromBeginning: true,
    });

    await this.consumer.subscribe({
      topic: 'orders.retry.5m',
      fromBeginning: true,
    });

    await this.consumer.subscribe({
      topic: 'orders.retry.10m',
      fromBeginning: true,
    });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const value = message.value?.toString();

        const scheduledRetryAt =
          message.headers?.scheduled_retry_at?.toString();

        console.log('Retry scheduler received message:', value);

        console.log('Scheduled retry at:', scheduledRetryAt);

        if (!scheduledRetryAt) {
          console.log('No scheduled retry time found. Skipping message.');

          return;
        }

        const scheduledTime = new Date(scheduledRetryAt).getTime();

        const currentTime = Date.now();

        if (currentTime < scheduledTime) {
          console.log('Retry is not ready yet.');

          return;
        }

        console.log('Retry is ready. Processing message...');

        try {
          await this.orderProcessor.process(value ?? '');

          console.log('Retry processing succeeded.');
        } catch {
          console.log('Retry processing failed.');
        }
      },
    });

    console.log('Retry scheduler connected');
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();

    console.log('Retry scheduler disconnected');
  }
}
