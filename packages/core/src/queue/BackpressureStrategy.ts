export enum BackpressureStrategy {
  BLOCK_PRODUCER,
  DROP_OLDEST,
  DROP_NEWEST,
  ERROR,
}

export default BackpressureStrategy;
