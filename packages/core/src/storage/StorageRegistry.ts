import type { StorageAdapter } from "./StorageAdapter";
import { MemoryStorageAdapter } from "./MemoryStorageAdapter";

const memoryStorageRegistry = new Map<string, MemoryStorageAdapter<any>>();

/**
 * Get or create a MemoryStorageAdapter for the given queue name.
 * For Redis/Postgres, pass a storage instance directly to Queue/Worker constructors.
 */
export function getMemoryStorage<T>(queueName: string, capacity: number = 1000): StorageAdapter<T> {
  if (!memoryStorageRegistry.has(queueName)) {
    memoryStorageRegistry.set(queueName, new MemoryStorageAdapter<T>(capacity));
  }
  return memoryStorageRegistry.get(queueName) as StorageAdapter<T>;
}

export function clearMemoryStorageRegistry(): void {
  memoryStorageRegistry.clear();
}
