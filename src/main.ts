import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  // Kafka producer/consumer connect during AppModule init, before listen

  // Without this, NestJS does not call onModuleDestroy on SIGTERM/SIGINT,
  // so `docker stop` would kill the process before Kafka/Redis disconnect
  // cleanly or the retry scheduler's interval is cleared.
  app.enableShutdownHooks();

  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
