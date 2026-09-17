import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { hostname } from 'os';

@Injectable()
export class AppLogger {
  // Identifies which process emitted a log line. Falls back to the
  // container/host name so logs are still attributable across multiple
  // instances without any extra configuration.
  private readonly instanceId: string;

  constructor(configService: ConfigService) {
    this.instanceId = configService.get<string>('INSTANCE_ID') ?? hostname();
  }

  info(event: string, data: Record<string, unknown> = {}): void {
    console.log(
      JSON.stringify({
        level: 'info',
        event,
        timestamp: new Date().toISOString(),
        instanceId: this.instanceId,
        ...data,
      }),
    );
  }

  error(event: string, data: Record<string, unknown> = {}): void {
    console.error(
      JSON.stringify({
        level: 'error',
        event,
        timestamp: new Date().toISOString(),
        instanceId: this.instanceId,
        ...data,
      }),
    );
  }
}
