import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import { OrderProcessor } from '../orders/order.processor';

@Injectable()
export class RetryConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

  constructor(private readonly orderProcessor: OrderProcessor) {
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
      eachMessage: async ({ message }) => {
        const value = message.value?.toString();

        const retryCountHeader = message.headers?.retry_count?.toString();
        const retryCount = Number(retryCountHeader ?? '0');

        console.log(
          'Retry consumer received:',
          value,
          'offset:',
          message.offset,
        );
        console.log('Retry count:', retryCount);

        try {
          await this.orderProcessor.process(value ?? '');

          console.log('Retry processing succeeded');
        } catch (error) {
          console.log('Retry processing failed', error);
        }
      },
    });

    console.log('Retry consumer connected');
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();

    console.log('Retry consumer disconnected');
  }
}
