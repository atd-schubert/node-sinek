export const DEFAULT_DRAIN_INTV = 3000;

export const DEFAULT_RETRY_OPTIONS = {
  factor: 3,
  forever: true,
  maxTimeout: 30000, // 30 secs
  minTimeout: 1000, // 1 sec
  randomize: true,
  retries: 1000, // overwritten by forever
  unref: false,
};
