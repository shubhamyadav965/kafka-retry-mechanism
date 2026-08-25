import { Injectable } from '@nestjs/common';

// Nest scaffold leftover; order flow uses KafkaService, not this
@Injectable()
export class AppService {
  getHello(): string {
    return 'Hello World!';
  }
}
