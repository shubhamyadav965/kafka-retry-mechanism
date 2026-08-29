import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { KafkaService } from './kafka/kafka.service';
import { KafkaConsumer } from './kafka/kafka.consumer';
import { RetryConsumer } from './kafka/retry.consumer';
import { OrderProcessor } from './orders/orders.processor';

@Module({
  imports: [],
  controllers: [AppController],
  // Both must be providers: Nest calls onModuleInit (connect) on each
  providers: [
    AppService,
    KafkaService,
    KafkaConsumer,
    RetryConsumer,
    OrderProcessor,
  ],
})
export class AppModule {}
