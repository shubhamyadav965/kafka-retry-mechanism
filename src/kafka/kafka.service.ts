import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly producer: Producer;

  constructor() {
    this.kafka = new Kafka({
      clientId: 'kafka-retry-framework',
      brokers: ['localhost:9092'], // docker-compose advertised listener
    });

    this.producer = this.kafka.producer();
  }

  async onModuleInit() {
    // Connect at boot so POST /orders does not pay a first-request handshake
    await this.producer.connect();
    console.log('Kafka producer connected');
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
    console.log('Kafka producer disconnected');
  }

  async sendMessage(topic: string, message: any) {
    await this.producer.send({
      topic,
      messages: [
        {
          value: JSON.stringify(message), // Kafka payload is bytes, not objects
        },
      ],
    });
  }

  async sendToRetryTopic(value: string, retryCount: number) {
    await this.producer.send({
      topic: 'orders.retry',
      messages: [
        {
          value,
          headers: {
            retry_count: retryCount.toString(),
            max_retries: '3',
            orginal_topic: 'orders',
          },
        },
      ],
    });
  }
}
