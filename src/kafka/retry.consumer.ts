import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';

import { Consumer, Kafka } from 'kafkajs';

@Injectable()
export class RetryConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

  constructor() {
    this.kafka = new Kafka({
      clientId: 'order-retry-consumer',
      brokers: ['localhost:9092'],
    });

    this.consumer = this.kafka.consumer({
      groupId: 'order-retry-consumer-group',
    });
  }

  async onModuleInit() {
    await this.consumer.connect();

    await this.consumer.subscribe({
      topic: 'orders.retry',
      fromBeginning: true,
    });

    await this.consumer.run({
      eachMessage: ({ message }) => {
        const value = message.value?.toString();

        console.log('Retry consumer received:', value);

        return Promise.resolve();
      },
    });

    console.log('Retry consumer connected');
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();

    console.log('Retry consumer disconnected');
  }
}
