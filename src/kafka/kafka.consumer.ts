import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import { KafkaService } from './kafka.service';
import { OrderProcessor } from '../orders/orders.processor';

@Injectable()
export class KafkaConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

  constructor(
    private readonly kafkaService: KafkaService,
    private readonly orderProcessor: OrderProcessor,
  ) {
    this.kafka = new Kafka({
      clientId: 'order-consumer',
      brokers: ['localhost:9092'], // docker-compose advertised listener
    });

    // Same groupId = shared offsets; a new group re-reads the topic
    this.consumer = this.kafka.consumer({
      groupId: 'order-consumer-group',
    });
  }

  async onModuleInit() {
    await this.consumer.connect();

    await this.consumer.subscribe({
      topic: 'orders',
      fromBeginning: true, // replay existing messages, not only new ones
    });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const value = message.value?.toString();

        console.log('Received message:', value);

        // Temporary: force a failure so retry/DLQ behavior can be built next
        //throw new Error('Something went wrong while processing order');
        try {
          // Temporary failure simulation
          throw new Error('Something went wrong while processing order');
        } catch {
          console.log('Processing failed. Sending to retry topic...');
          await this.kafkaService.sendToRetryTopic(value ?? '');
        }
      },
    });

    console.log('Kafka consumer connected');
  }

  async onModuleDestroy() {
    await this.consumer.disconnect(); // leave the group cleanly on shutdown
  }
}
