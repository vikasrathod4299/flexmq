import { getMemoryStorage, clearMemoryStorageRegistry, MemoryStorageAdapter } from "flexmq";

describe("StorageRegistry", () => {
  beforeEach(() => {
    clearMemoryStorageRegistry();
  });

  describe("getMemoryStorage", () => {
    it("should return same instance for same queue name", () => {
      const storage1 = getMemoryStorage("test-queue");
      const storage2 = getMemoryStorage("test-queue");

      expect(storage1).toBe(storage2);
    });

    it("should return different instances for different queue names", () => {
      const storage1 = getMemoryStorage("queue-1");
      const storage2 = getMemoryStorage("queue-2");

      expect(storage1).not.toBe(storage2);
    });

    it("should return MemoryStorageAdapter", () => {
      const storage = getMemoryStorage("test-queue");
      expect(storage).toBeInstanceOf(MemoryStorageAdapter);
    });
  });

  describe("clearMemoryStorageRegistry", () => {
    it("should clear all cached instances", () => {
      const storage1 = getMemoryStorage("test-queue");
      clearMemoryStorageRegistry();
      const storage2 = getMemoryStorage("test-queue");

      expect(storage1).not.toBe(storage2);
    });
  });
});
