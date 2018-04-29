export type LoggerFn = (message: string, payload?: any) => void;

export interface ILogger {
  debug: LoggerFn;
  info: LoggerFn;
  warn: LoggerFn;
  error: LoggerFn;
}
