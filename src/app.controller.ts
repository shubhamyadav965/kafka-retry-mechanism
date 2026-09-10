import { Controller, Post } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { KafkaService } from './kafka/kafka.service';

@Controller()
export class AppController {
  constructor(private readonly kafkaService: KafkaService) {}

  @Post('orders')
  async createOrder() {
    // Generate one stable ID for this business event.
    // All retries of this event will keep the same eventId.
    const eventId = randomUUID();

    // Sample payload until a real request DTO exists
    const order = {
      event_id: eventId,
      event_name: 'ORDER_CREATED',
      order_id: 'ORD-1001',
      user_id: 'USER-101',
      amount: 500,
    };

    // Publish only; the consumer processes this asynchronously
    await this.kafkaService.sendMessage('orders', order);

    return {
      message: 'Order event sent to Kafka',
      order,
    };
  }
}
