import { EventEmitter } from "events";
import { BoundedQueue } from "./BoundedQueue";
import BackpressureStrategy from "./BackpressureStrategy";

class AsyncQueue<T> extends EventEmitter {
  private queue: BoundedQueue<T>;
  private strategy: BackpressureStrategy;
  private waitingConsumers: Array<(value: T) => void> = [];
  private waitingProducers: Array<{ item: T; resolve: (value: boolean) => void }> = [];

  constructor(
    capacity: number = 1000,
    strategy: BackpressureStrategy = BackpressureStrategy.BLOCK_PRODUCER
  ) {
    super();
    this.queue = new BoundedQueue<T>(capacity);
    this.strategy = strategy;
  }

  async enqueue(item: T): Promise<boolean> {
    if (this.waitingConsumers.length > 0) {
      const resolver = this.waitingConsumers.shift()!;
      resolver(item);
      return true;
    }

    if (this.queue.isFull) {
      switch (this.strategy) {
        case BackpressureStrategy.DROP_NEWEST:
          this.emit("queue:dropped", { reason: "DROP_NEWEST strategy applied" } as any);
          return false;
        case BackpressureStrategy.DROP_OLDEST:
          this.queue.pop();
          this.queue.push(item);
          this.emit("queue:dropped", { reason: "DROP_OLDEST strategy applied" } as any);
          return true;
        case BackpressureStrategy.BLOCK_PRODUCER:
          return new Promise<boolean>((resolve) => {
            this.waitingProducers.push({ item, resolve });
          });
        case BackpressureStrategy.ERROR:
          throw new Error("Queue is full");
      }
    }

    this.queue.push(item);
    return true;
  }

  async dequeue(): Promise<T> {
    if (this.queue.isEmpty) {
      this.emit("queue:drained");
      return new Promise<T>((resolve) => {
        this.waitingConsumers.push(resolve);
      });
    }

    const item = this.queue.pop()!;

    if (this.waitingProducers.length > 0) {
      const { item: producerItem, resolve } = this.waitingProducers.shift()!;
      this.queue.push(producerItem);
      resolve(true);
    }

    return item;
  }

  size(): number {
    return this.queue.size;
  }

  isFull(): boolean {
    return this.queue.isFull;
  }

  isEmpty(): boolean {
    return this.queue.isEmpty;
  }

  peek(): T | undefined {
    return this.queue.peek;
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<T> {
    const self = this;
    return {
      async next(): Promise<IteratorResult<T>> {
        const value = await self.dequeue();
        return { value, done: false };
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }
}

export { AsyncQueue };
