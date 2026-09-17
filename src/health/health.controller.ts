import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { RedisService } from '../redis/redis.service';
import { KafkaService } from '../kafka/kafka.service';

@Controller('health')
export class HealthController {
  constructor(
    private readonly redisService: RedisService,
    private readonly kafkaService: KafkaService,
  ) {}

  @Get()
  async check() {
    const redisOk = await this.redisService
      .ping()
      .then(() => true)
      .catch(() => false);
    const kafkaOk = this.kafkaService.isProducerConnected();

    const body = {
      status: redisOk && kafkaOk ? 'ok' : 'degraded',
      dependencies: {
        redis: redisOk ? 'ok' : 'error',
        kafka: kafkaOk ? 'ok' : 'error',
      },
    };

    // 503 tells orchestrators/load balancers not to route traffic here,
    // matching the Docker healthcheck's expectation of a non-2xx status.
    if (!redisOk || !kafkaOk) {
      throw new ServiceUnavailableException(body);
    }

    return body;
  }
}
