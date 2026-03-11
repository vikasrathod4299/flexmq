export class BoundedQueue<T> {
  private items: T[];
  private capacity: number;

  constructor(capacity: number) {
    if (capacity <= 0) {
      throw new Error("Capacity must be greater than 0");
    }
    this.capacity = capacity;
    this.items = [];
  }

  push(item: T): boolean {
    if (this.isFull) return false;
    this.items.push(item);
    return true;
  }

  pop(): T | undefined {
    return this.items.shift();
  }

  get size(): number {
    return this.items.length;
  }

  get isFull(): boolean {
    return this.size >= this.capacity;
  }

  get isEmpty(): boolean {
    return this.size === 0;
  }

  get peek(): T | undefined {
    return this.items[0];
  }
}
