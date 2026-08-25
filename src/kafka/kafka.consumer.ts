import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';

@Injectable()
export class KafkaConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

  constructor() {
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
      eachMessage: ({ message }) => {
        const value = message.value?.toString();

        console.log('Received message:', value);

        // Temporary: force a failure so retry/DLQ behavior can be built next
        throw new Error('Something went wrong while processing order');
      },
    });

    console.log('Kafka consumer connected');
  }

  async onModuleDestroy() {
    await this.consumer.disconnect(); // leave the group cleanly on shutdown
  }
}
