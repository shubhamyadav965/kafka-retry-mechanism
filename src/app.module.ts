import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { KafkaService } from './kafka/kafka.service';
import { KafkaConsumer } from './kafka/kafka.consumer';
import { RetryConsumer } from './kafka/retry.consumer';
import { OrderProcessor } from './orders/order.processor';
import { RetryScheduler } from './retry/retry.scheduler';
import { RedisModule } from './redis/redis.module';

@Module({
  // RedisModule is a module (exports RedisService), so it belongs in
  // imports, not providers.
  imports: [RedisModule],
  controllers: [AppController],
  // Providers are classes managed by NestJS dependency injection.
  //
  // KafkaService       → produces Kafka messages
  // KafkaConsumer      → consumes normal orders
  // RetryConsumer      → consumes failed orders
  // OrderProcessor     → contains business processing logic
  // RetryScheduler     → re-processes retry messages once due
  providers: [
    AppService,
    KafkaService,
    KafkaConsumer,
    RetryConsumer,
    OrderProcessor,
    RetryScheduler,
  ],
})
export class AppModule {}
