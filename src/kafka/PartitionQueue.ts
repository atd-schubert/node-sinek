"use strict";

import {AsyncQueue, queue} from "async";
import { PartitionDrainer } from "./PartitionDrainer";

export interface IPartitionQueueStats {
  lastProcessed: number;
  lastProcessedOffset: number;
  lastPushed: number;
  partition: number;
  queueSize: number | null;
  totalProcessFails: number;
  totalProcessed: number;
  totalPushed: number;
  workers: number;
}

export type DrainEventCallback = (partition: number, lastOffset: number) => void;

export class PartitionQueue {

  private q: AsyncQueue<any> | null = null;

  private lastProcessed: number = Date.now();
  private lastPushed: number = Date.now();

  private totalPushed: number = 0;
  private totalProcessedMessages: number = 0;
  private totalMessageProcessFails: number = 0;
  private lastOffset: number = -1;

  private drainCheckIntv: NodeJS.Timer | null = null;

  constructor(
    public partition: number,
    private drainEvent: (...args: any[]) => void,
    private drainer: PartitionDrainer,
    public asyncLimit: number = 1,
    private onQueueDrain: DrainEventCallback | null = null,
  ) {

    if (typeof drainEvent !== "function") {
      throw new Error("drainEvent must be a function.");
    }

    if (typeof onQueueDrain !== "function") {
      throw new Error("queueDrain must be a function.");
    }
  }

  public push(message: any) {
    if (this.q) {
      this.totalPushed++;
      this.lastPushed = Date.now();
      this.q.push(message);
    }
  }

  public getLastProcessed() {
    return this.lastProcessed;
  }

  public getStats(): IPartitionQueueStats {
    return {
      lastProcessed: this.lastProcessed,
      lastProcessedOffset: this.lastOffset,
      lastPushed: this.lastPushed,
      partition: this.partition,
      queueSize: this.q ? this.q.length() : null,
      totalProcessFails: this.totalMessageProcessFails,
      totalProcessed: this.totalProcessedMessages,
      totalPushed: this.totalPushed,
      workers: this.asyncLimit,
    };
  }

  public build() {

    if (this.q) {
      throw new Error("this queue has already been build.");
    }

    this.q = queue((msg, done) => {
      if (this.drainEvent) {
        setImmediate(() => this.drainEvent(msg, (err: Error) => {

          try {
            if (typeof msg.offset === "undefined") {
              if (!err) {
                err = new Error("missing offset on message: " + JSON.stringify(msg));
              }
              this._getLogger().error("missing offset on message: " + JSON.stringify(msg));
            } else {
              this.lastOffset = msg.offset;
            }
          } catch (e) {
            if (!err) {
              err = new Error("failed to parse message offset: " + e);
            }
            this._getLogger().error("failed to parse message offset: " + e);
          }

          this.lastProcessed = Date.now();
          this.totalProcessedMessages++;
          done(err);
        }));
      } else {
        this._getLogger().debug("drainEvent not present, message is dropped.");
      }
    }, this.asyncLimit);

    this.q.drain = () => {
      this._emitDrain();
    };

    this.q.error((err: Error) => {
      if (err) {
        this.totalMessageProcessFails++;
        this._getLogger().warn("error was passed back to consumer queue, dropping it silently: " + JSON.stringify(err));
      }
    });

    this._getLogger().info(`partition queue has been build for partition: ${ this.partition }.`);
    this._runDrainCheckIntv();
    return this; // chain?
  }

  public close() {

    this._closeDrainCheckIntv();

    if (this.q) {
      this.q.kill();
      this.q = null;
    }

    this._getLogger().info("queue closed.");
  }

  private _getLogger() {
    return this.drainer._getLogger();
  }

  private _emitDrain() {
    if (this.onQueueDrain) {
      process.nextTick(() => { // await potential writing of lastOffset
        this.onQueueDrain!(this.partition, this.lastOffset);
      });
    }
  }

  private _runDrainCheckIntv(ms = 500, drainSpan = 2500) {

    if (this.drainCheckIntv) {
      throw new Error("drain check interval already active for partition queue.");
    }

    this.drainCheckIntv = setInterval(() => {

      if (!this.q) {
        return;
      }

      // queue size is greater than 0, will emit own drain event soon anyway
      if (this.q.length() > 0) {
        return;
      }

      if (Date.now() - this.lastPushed > drainSpan) {
        this._getLogger().debug(`partition ${this.partition} received no messages, flushing queue anyway.`);
        this._emitDrain();
      }
    }, ms);
  }

  private _closeDrainCheckIntv() {
    if (this.drainCheckIntv) {
      clearInterval(this.drainCheckIntv);
    }
  }
}
