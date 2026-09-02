import { Controller, Get, Post } from '@nestjs/common';
import { KafkaService } from './kafka/kafka.service';
import { RedisService } from './redis/redis.service';

@Controller()
export class AppController {
  constructor(
    private readonly kafkaService: KafkaService,
    private readonly redisService: RedisService,
  ) {}

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

  // Temporary: verifies the Redis connection before wiring it
  // into the retry system.
  @Get('/redis-test')
  async redisTest() {
    await this.redisService.set('test:key', 'hello redis');

    const value = await this.redisService.get('test:key');

    return {
      value,
    };
  }
}
