import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';

import { Consumer, Kafka } from 'kafkajs';

import { OrderProcessor } from '../orders/order.processor';
import { KafkaService } from './kafka.service';

@Injectable()
export class RetryConsumer implements OnModuleInit, OnModuleDestroy {
  // Kafka client used by the retry consumer.
  private readonly kafka: Kafka;

  // Consumer responsible for reading failed messages
  // from the retry topic.
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
      // Separate consumer group because retry messages
      // are processed independently from normal orders.
      groupId: 'order-retry-consumer-group',
    });
  }

  async onModuleInit() {
    // Connect retry consumer to Kafka.
    await this.consumer.connect();

    // Listen to messages from the retry topic.
    await this.consumer.subscribe({
      topic: 'orders.retry',

      // Useful during development so we can replay
      // messages already present in the topic.
      fromBeginning: true,
    });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        // Convert Kafka message value from Buffer to string.
        const value = message.value?.toString();

        // Read retry_count from Kafka headers.
        const retryCountHeader = message.headers?.retry_count?.toString();

        // Convert the header from string to number.
        const retryCount = Number(retryCountHeader ?? '0');

        // Read maximum retry count from Kafka headers.
        const maxRetriesHeader = message.headers?.max_retries?.toString();

        const maxRetries = Number(maxRetriesHeader ?? '3');

        console.log(
          'Retry consumer received:',
          value,
          'offset:',
          message.offset,
        );

        console.log('Retry count:', retryCount, 'Max retries:', maxRetries);

        try {
          // Try processing the failed message again.
          await this.orderProcessor.process(value ?? '');

          console.log('Retry processing succeeded');
        } catch (error) {
          console.log('Retry processing failed');

          // If we have not reached the maximum retry count,
          // create another retry message.
          if (retryCount < maxRetries) {
            const nextRetryCount = retryCount + 1;

            console.log(`Sending message for retry ${nextRetryCount}`);

            await this.kafkaService.sendToRetryTopic(
              value ?? '',
              nextRetryCount,
            );
          } else {
            // Maximum retries have been exhausted.
            // Send the message to the Dead Letter Queue.
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
    // Gracefully disconnect from Kafka when NestJS shuts down.
    await this.consumer.disconnect();

    console.log('Retry consumer disconnected');
  }
}
