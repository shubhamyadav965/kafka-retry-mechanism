import { Injectable } from '@nestjs/common';

export interface MetricsSnapshot {
  messages_received: number;
  messages_processed: number;
  messages_failed: number;
  retries_scheduled: number;
  retries_processed: number;
  duplicate_events: number;
  dlq_messages: number;
  retry_latency_total_ms: number;
  retry_latency_count: number;
}

@Injectable()
export class MetricsService {
  private readonly counters: MetricsSnapshot = {
    messages_received: 0,
    messages_processed: 0,
    messages_failed: 0,
    retries_scheduled: 0,
    retries_processed: 0,
    duplicate_events: 0,
    dlq_messages: 0,
    retry_latency_total_ms: 0,
    retry_latency_count: 0,
  };

  increment(metric: keyof MetricsSnapshot, value = 1): void {
    this.counters[metric] += value;
  }

  recordRetryLatency(latencyMs: number): void {
    this.counters.retry_latency_total_ms += latencyMs;
    this.counters.retry_latency_count += 1;
  }

  getMetrics() {
    const averageRetryLatencyMs =
      this.counters.retry_latency_count === 0
        ? 0
        : this.counters.retry_latency_total_ms /
          this.counters.retry_latency_count;

    return {
      ...this.counters,
      average_retry_latency_ms: averageRetryLatencyMs,
    };
  }

  reset(): void {
    this.counters.messages_received = 0;
    this.counters.messages_processed = 0;
    this.counters.messages_failed = 0;
    this.counters.retries_scheduled = 0;
    this.counters.retries_processed = 0;
    this.counters.duplicate_events = 0;
    this.counters.dlq_messages = 0;
    this.counters.retry_latency_total_ms = 0;
    this.counters.retry_latency_count = 0;
  }
}
