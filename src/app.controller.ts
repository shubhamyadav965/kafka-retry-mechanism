import { Controller, Post } from '@nestjs/common';
import { KafkaService } from './kafka/kafka.service';

@Controller()
export class AppController {
  constructor(private readonly kafkaService: KafkaService) {}

  @Post('orders')
  async createOrder() {
    // Sample payload until a real request DTO exists
    const order = {
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
