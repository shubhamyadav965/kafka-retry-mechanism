import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import { OrderProcessor } from '../orders/order.processor';
import { KafkaService } from './kafka.service';
import { RetryService } from '../retry/retry.service';
import { getMaxRetries, getRetryTopic } from '../config/retry-policy';

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
    private readonly retryService: RetryService,
  ) {
    this.kafka = new Kafka({
      clientId: 'order-retry-consumer',
      brokers: ['kafka:9093'],
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

    // Listen to messages across all tiered retry topics
    // (orders.retry.1m, orders.retry.5m, orders.retry.10m).
    const retryTopics = Array.from({ length: getMaxRetries() }, (_, i) =>
      getRetryTopic('orders', i + 1),
    );

    await this.consumer.subscribe({
      topics: retryTopics,

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

        const maxRetries = Number(
          maxRetriesHeader ?? getMaxRetries().toString(),
        );

        const originalTopic =
          message.headers?.original_topic?.toString() ?? 'unknown';

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

            console.log(`Scheduling retry ${nextRetryCount}`);

            await this.retryService.scheduleRetry(
              value ?? '',
              nextRetryCount,
              originalTopic,
            );

            console.log('Retry job stored in Redis');
          } else {
            // Maximum retries have been exhausted.
            // Send the message to the Dead Letter Queue.
            console.log('Maximum retries reached. Sending to DLQ.');

            await this.kafkaService.sendToDlq(
              value ?? '',
              retryCount,
              error instanceof Error ? error.message : 'Unknown error',
              originalTopic,
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
