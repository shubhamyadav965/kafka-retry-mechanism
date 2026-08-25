import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  // Kafka producer/consumer connect during AppModule init, before listen
  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
