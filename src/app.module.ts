import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { KafkaService } from './kafka/kafka.service';
import { KafkaConsumer } from './kafka/kafka.consumer';
import { RetryConsumer } from './kafka/retry.consumer';
import { OrderProcessor } from './orders/order.processor';
import { RetryScheduler } from './retry/retry.scheduler';
import { RedisModule } from './redis/redis.module';
import { RetryService } from './retry/retry.service';
import { IdempotencyModule } from './idempotency/idempotency.module';
import { LoggerModule } from './common/logger/logger.module';
import { MetricsModule } from './common/metrics/metrics.module';
import { ConfigModule } from '@nestjs/config';
import { validateEnv } from './config/env.validation';
import { HealthController } from './health/health.controller';

@Module({
  // RedisModule is a module (exports RedisService), so it belongs in
  // imports, not providers.
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      // Fails startup immediately with a clear message on missing/invalid
      // config, instead of surfacing as a confusing runtime error later.
      validate: validateEnv,
    }),
    RedisModule,
    IdempotencyModule,
    LoggerModule,
    MetricsModule,
  ],
  // HealthController lives here (rather than its own module) because it
  // needs both RedisService (via RedisModule, imported above) and
  // KafkaService (a provider below) — the same shape as AppController.
  controllers: [AppController, HealthController],
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
    RetryService,
  ],
})
export class AppModule {}
