import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import { OrderProcessor } from '../orders/order.processor';
import { KafkaService } from './kafka.service';

@Injectable()
export class RetryConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

  constructor(
    private readonly orderProcessor: OrderProcessor,
    private readonly kafkaService: KafkaService,
  ) {
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
        const maxRetriesHeader = message.headers?.max_retries?.toString();
        const retryCount = Number(retryCountHeader ?? '0');
        const maxRetries = Number(maxRetriesHeader ?? '3');

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

          if (retryCount < maxRetries) {
            const nextRetryCount = retryCount + 1;

            await this.kafkaService.sendToRetryTopic(
              value ?? '',
              nextRetryCount,
            );
          } else {
            console.log('Maximum retries reached. Sending to DLQ.');

            await this.kafkaService.sendToDlq(
              value ?? '',
              retryCount,
              error instanceof Error ? error.message : 'Unknown error',
            );
          }
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
