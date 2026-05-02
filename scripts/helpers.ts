import os from "node:os";
import process from "node:process";

export type BenchmarkLatency = {
  p50: number;
  p95: number;
  p99: number;
};

export type BenchmarkResult = {
  scenario: string;
  adapter: string;
  jobs: number;
  elapsedMs: number;
  throughputPerSec: number;
  latencyMs?: BenchmarkLatency;
  notes?: string[];
  environment: {
    node: string;
    platform: string;
    cpus: number;
    hostname: string;
  };
};

export type StressResult = {
  scenario: string;
  adapter: string;
  durationMs: number;
  summary: Record<string, number | string | boolean>;
  invariants: Array<{ name: string; passed: boolean; detail?: string }>;
  environment: BenchmarkResult["environment"];
};

export function nowMs(): number {
  return Number(process.hrtime.bigint() / BigInt(1_000_000));
}

export function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return round(sorted[index] ?? 0);
}

export function latencySummary(values: number[]): BenchmarkLatency {
  return {
    p50: percentile(values, 50),
    p95: percentile(values, 95),
    p99: percentile(values, 99),
  };
}

export function throughputPerSec(count: number, elapsedMs: number): number {
  if (elapsedMs <= 0) return 0;
  return round((count / elapsedMs) * 1000);
}

export function round(value: number): number {
  return Math.round(value * 100) / 100;
}

export function environmentInfo() {
  return {
    node: process.version,
    platform: `${process.platform}-${process.arch}`,
    cpus: os.cpus().length,
    hostname: os.hostname(),
  };
}

export function printJsonResult(result: BenchmarkResult | StressResult): void {
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

export function parseIntArg(name: string, defaultValue: number): number {
  const arg = process.argv.find((value) => value.startsWith(`--${name}=`));
  if (!arg) return defaultValue;
  const value = Number(arg.split("=")[1]);
  return Number.isFinite(value) ? value : defaultValue;
}

export function parseStringArg(name: string, defaultValue: string): string {
  const arg = process.argv.find((value) => value.startsWith(`--${name}=`));
  if (!arg) return defaultValue;
  return arg.split("=")[1] ?? defaultValue;
}

export async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
