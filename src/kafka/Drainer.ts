"use strict";

import {AsyncQueue, queue} from "async";
import Promise from "bluebird";
import { ConsumerGroup } from "kafka-node";
import { DEFAULT_DRAIN_INTV, NOOP } from "../common";
import { Kafka } from "./Kafka";

export interface IMessage {
  value: string;
  offset: number;
  partition: number;
  topic: string;
}

export interface IDrainStats {
  [name: string]: number | undefined;
  "last-proc-since"?: number;
  "last-rec-since"?: number;
}

export interface IStats {
  totalIncoming: number;
  lastMessage: number;

  receivedFirstMsg: boolean;

  totalProcessed: number;
  lastProcessed: number; // Timestamp

  queueSize: number | null;

  isPaused: boolean | null;

  drainStats: IDrainStats;
  omittingQueue: boolean;
}

export class Drainer {
  // TODO: maybe as protected?
  public raw: ConsumerGroup;

  private drainEvent?: (...args: any[]) => void;
  private q?: AsyncQueue<any>; // TODO: any?

  private lastProcessed: number = Date.now();
  private lastReceived: number = Date.now();

  private totalIncomingMessages: number = 0;
  private totalProcessedMessages: number = 0;

  private messagesSinceLastDrain: number = 0;
  private receivedFirst: boolean = false;
  private drainStart?: number;

  private stats: IDrainStats = {};

  private lastMessageHandlerRef = null;

  private DRAIN_INTV = DEFAULT_DRAIN_INTV;

  constructor(
    public consumer: Kafka,
    public asyncLimit: number = 1,
    public autoJsonParsing: boolean = true,
    public omitQueue = false,
    public commitOnDrain = false,
  ) {

    if (!consumer || !(consumer! instanceof Kafka) || !consumer!.isConsumer) {
      throw new Error("Consumer is not a valid Sinek Kafka(Consumer)");
    }
    if (!consumer.consumer) {
      throw new Error("Consumer is not configured and initialized");
    }

    if (omitQueue && commitOnDrain) {
      throw new Error("Cannot run drain commits when queue is omitted. Please either run: " +
        " a manual committer with backpressure OR an auto-commiter without backpressure.");
    }

    this.raw = this.consumer.consumer as ConsumerGroup;
  }

  /**
   * stops any active drain process
   * closes the consumer and its client
   */
  public close() {

    if (this.lastMessageHandlerRef) {
      this.raw.removeListener("message", this.lastMessageHandlerRef);
      this.lastMessageHandlerRef = null;
    } else {
      this.getLogger().warn("message handler ref not present during close, could not remove listener.");
    }

    this.getLogger().info("[Drainer] closed.");
    return this.consumer.close();
  }

  /**
   * returns a few insights
   * @returns {{totalIncoming: number, last: (number|*), isPaused: *}}
   */
  public getStats(): IStats {
    return {
      lastMessage: this.lastReceived,
      totalIncoming: this.totalIncomingMessages,

      receivedFirstMsg: this.receivedFirst,

      lastProcessed: this.lastProcessed,
      totalProcessed: this.totalProcessedMessages,

      queueSize: this.q ? this.q.length() : null,

      isPaused: this.consumer && this.consumer.isConsumer ? this.isPaused() : null,

      drainStats: this.stats,
      omittingQueue: this.omitQueue,
    };
  }

  /**
   * resets all offsets and starts from being
   * also un-pauses consumer if necessary
   * @param topics
   * @returns {Promise.<TResult>}
   * @deprecated
   */
  public resetConsumer() {
    return Promise.reject("resetConsumer has been removed, due to supporting bad kafka consumer behaviour.");
  }

  /**
   * resets all offsets and makes sure the consumer is paused
   * @param topics
   * @returns {Promise.<TResult>}
   * @deprecated
   */
  public resetOffset() {
    return Promise.reject("resetOffset has been removed, due to supporting bad kafka consumer behaviour.");
  }

  /**
   * main reg. function, pass it a function to receive messages
   * under flow control
   * @param drainEvent
   */
  public drain(drainEvent: (...args: any[]) => void) {

    // if (!drainEvent || typeof drainEvent !== "function") {
    //   throw new Error("drainEvent must be a valid function");
    // }

    if (this.drainEvent) {
      throw new Error("a drain process is currently active.");
    }

    // reset
    this.lastProcessed = Date.now();
    this.lastReceived = Date.now();
    this.stats = {};

    this.messagesSinceLastDrain = this.totalIncomingMessages;
    this.drainEvent = drainEvent;
    this.startToReceiveMessages();

    if (this.isPaused()) {
      this.resume();
    }
  }

  /**
   * main req. function, pass it a function to receive messages
   * under flow control, until they are stall for a certain amount
   * of time (e.g. when all messages on the queue are consumed)
   * returns a Promise
   * @param drainEvent
   * @param drainThreshold
   * @param timeout
   */
  public drainOnce(drainEvent: (...args: any[]) => void, drainThreshold: number = 10000, timeout: number = 0) {
    return new Promise((resolve, reject) => {

      // if (!drainEvent || typeof drainEvent !== "function") {
      //   return reject("drainEvent must be a valid function");
      // }

      if (this.drainEvent) {
        return reject("a drain process is currently active.");
      }

      if (timeout !== 0 && timeout < this.DRAIN_INTV) {
        return reject(`timeout must be either 0 or > ${this.DRAIN_INTV}.`);
      }

      if (timeout !== 0 && timeout <= drainThreshold) {
        return reject(`timeout ${timeout} must be greater than the drainThreshold ${drainThreshold}.`);
      }

      let t: NodeJS.Timer;
      let intv: NodeJS.Timer;

      intv = setInterval(() => {

        const spanProcessed = Date.now() - this.lastProcessed;
        const spanReceived = Date.now() - this.lastReceived;

        this.getLogger().debug("drainOnce interval running, current span-rec: " +
          `${spanReceived} / span-proc: ${spanProcessed} ms.`);

        // set stats
        this.countStats("intv-cycle");
        this.stats["last-proc-since"] = spanProcessed;
        this.stats["last-rec-since"] = spanReceived;

        // choose the smaller span
        const span = spanProcessed < spanReceived ? spanProcessed : spanReceived;

        if (span >= drainThreshold) {
          this.getLogger().info(`drainOnce span ${span} hit threshold ${drainThreshold}.`);
          clearInterval(intv);
          clearTimeout(t);
          this.stopDrain();
          resolve(this.totalIncomingMessages - this.messagesSinceLastDrain);
        }
      }, this.DRAIN_INTV);

      if (timeout !== 0) {
        this.getLogger().info(`drainOnce timeout active: ${timeout} ms.`);
        t = setTimeout(() => {
          this.getLogger().warn(`drainOnce timeout hit after ${timeout} ms.`);
          clearInterval(intv);
          this.stopDrain();
          reject("drainOnce ran into timeout.");
        }, timeout);
      }

      // start the drain process
      this.drain(drainEvent);
    });
  }

  /**
   * stops any active drain process
   */
  public stopDrain() {

    if (!this.drainEvent) {
      throw new Error("there is no drain active.");
    }

    // reset
    if (this.lastMessageHandlerRef) {
      this.raw.removeListener("message", this.lastMessageHandlerRef);
      this.lastMessageHandlerRef = null;
    } else {
      this.getLogger().warn("message handler ref not present during close, could not remove listener.");
    }

    this.drainEvent = undefined;
    this.q = undefined;
    this.receivedFirst = false;

    const duration = (Date.now() - this.drainStart!) / 1000;
    this.getLogger().info(`[Drainer] stopped drain process, had been open for ${duration} seconds.`);
  }

  /**
   * removes kafka topics (if broker allows this action)
   * @param topics
   */
  public removeTopics(topics: string[] = []) {
    return new Promise((resolve, reject) => {
      this.getLogger().info(`deleting topics ${JSON.stringify(topics)}.`);
      this.raw.client.removeTopicMetadata(topics, (err: Error, data: any) => {

        if (err) {
          return reject(err);
        }

        resolve(data);
      });
    });
  }

  public pause() {
    if (!this.isPaused()) {
      this.countStats("paused");
    }

    return this.consumer.pause();
  }

  public resume() {

    if (this.isPaused()) {
      this.countStats("resumed");
    }

    return this.consumer.resume();
  }

  public isPaused() {
    return this.consumer.isPaused();
  }

  /**
   * consumer proxy
   */
  public on(event: string | symbol, listener: (...args: any[]) => void): this {
    this.consumer.on(event, listener);
    return this;
  }

  /**
   * consumer proxy
   */
  public once(event: string | symbol, listener: (...args: any[]) => void): this {
    this.consumer.once(event, listener);
    return this;
  }

  /**
   * consumer proxy
   */
  public removeListener(event: string | symbol, listener: (...args: any[]) => void): this {
    this.consumer.removeListener(event, listener);
    return this;
  }

  /**
   * consumer proxy
   */
  public emit(event: string | symbol, ...args: any[]): boolean {
    return this.consumer.emit(event, ...args);
  }

  private getLogger() {
    // Access to private member!
    return (this.consumer as any).getLogger();
  }

  private startToReceiveMessages() {

    if (!this.omitQueue) {
      this.startToReceiveMessagesThroughQueue();
    } else {
      this.startToReceiveMessagesWithoutQueue();
    }
  }

  private startToReceiveMessagesThroughQueue() {

    this.q = queue((msg, done) => {
      if (this.drainEvent) {
        setImmediate(() => this.drainEvent!(msg, (err: Error) => {
          this.lastProcessed = Date.now();
          this.totalProcessedMessages++;
          done(err);
        }));
      } else {
        this.getLogger().debug("drainEvent not present, message is dropped.");
      }
    }, this.asyncLimit);

    this.q.drain = () => {

      if (!this.commitOnDrain) {
        return this.resume();
      }

      // commit state first, before resuming
      this.getLogger().debug("committing manually, reason: drain event.");
      this.commit().then(() => {
        this.getLogger().debug("committed successfully, resuming.");
        this.resume();
      }).catch((error: Error) => {
        this.getLogger().error("failed to commit offsets, resuming anyway after: " + error);
        this.resume();
      });
    };

    this.q.error((err: Error) => {
      if (err) {
        this.countStats("msg-process-fail");
        this.getLogger().warn("error was passed back to consumer queue, dropping it silently: " + JSON.stringify(err));
      }
    });

    this.lastMessageHandlerRef = this.onMessageForQueue.bind(this);
    this.raw.on("message", this.lastMessageHandlerRef);
    this.getLogger().info("[Drainer] started drain process.");
    this.drainStart = Date.now();
  }

  private commit() {
    return this.consumer.commitCurrentOffsets();
  }

  private startToReceiveMessagesWithoutQueue() {

    this.lastMessageHandlerRef = this.onMessageNoQueue.bind(this);
    this.raw.on("message", this.lastMessageHandlerRef);
    this.getLogger().info("[Drainer] started drain process.");
    this.drainStart = Date.now();
  }

  /**
   * with backpressure
   * @param {*} message
   */
  private onMessageForQueue(message: IMessage) {

    this.getLogger().debug(
      "received kafka message => length: " +
      (message.value && message.value.length) +
      ", offset: " +
      message.offset +
      ", partition: " +
      message.partition +
      ", on topic: " +
      message.topic,
    );

    if (this.autoJsonParsing) {
      try {
        message.value = JSON.parse(message.value);
      } catch (e) {
        this.countStats("msg-parse-fail");
        return this.emit("error", "failed to json parse message value: " + message);
      }

      if (!message.value) {
        this.countStats("msg-empty");
        return this.emit("error", "message value is empty: " + message);
      }
    }

    if (!this.q) {
      throw new Error("No queue available, maybe you forget to call .startToReceiveMessagesThroughQueue() first");
    }

    this.q.push(message);
    // error handling happens directly on the queue object initialisation

    this.pause();

    this.totalIncomingMessages++;
    this.lastReceived = Date.now();

    if (!this.receivedFirst) {
      this.receivedFirst = true;
      this.getLogger().info("consumer received first message.");
      this.emit("first-drain-message", message);
    }
  }

  /**
   * no backpressure
   * @param {*} message
   */
  private onMessageNoQueue(message: IMessage) {

    this.getLogger().debug(
      "received kafka message => length: " +
      (message.value && message.value.length) +
      ", offset: " +
      message.offset +
      ", partition: " +
      message.partition +
      ", on topic: " +
      message.topic,
    );

    if (this.autoJsonParsing) {
      try {
        message.value = JSON.parse(message.value);
      } catch (e) {
        this.countStats("msg-parse-fail");
        return this.emit("error", "failed to json parse message value: " + message);
      }

      if (!message.value) {
        this.countStats("msg-empty");
        return this.emit("error", "message value is empty: " + message);
      }
    }

    this.totalIncomingMessages++;
    this.lastReceived = Date.now();

    if (this.drainEvent) {
      this.drainEvent(message, NOOP);
      this.lastProcessed = Date.now();
      this.totalProcessedMessages++;
    }

    if (!this.receivedFirst) {
      this.receivedFirst = true;
      this.getLogger().info("consumer received first message.");
      this.emit("first-drain-message", message);
    }
  }

  private countStats(key: string) {

    if (!this.stats) {
      return;
    }

    if (!this.stats[key]) {
      this.stats[key] = 1;
      return;
    }

    this.stats[key]!++;
  }
}
