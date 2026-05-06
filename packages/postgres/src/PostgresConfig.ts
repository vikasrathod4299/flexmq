import type { PoolConfig } from "pg";

export interface PostgresConfig {
  connectionString?: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  ssl?: PoolConfig["ssl"];
  capacity: number;
}
