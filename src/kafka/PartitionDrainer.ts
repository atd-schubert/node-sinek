"use strict";

import Promise from "bluebird";
import { ConsumerGroup } from "kafka-node";
import { DEFAULT_DRAIN_INTV } from "../common";
import { Kafka } from "./Kafka";
import {DrainEventCallback, IPartitionQueueStats, PartitionQueue} from "./PartitionQueue";

export interface IPartitionDrainerStats {
  lastMessage: number;
  totalIncoming: number;
  receivedFirstMsg: boolean;
  isPaused: boolean | null;
  drainStats: IDrainerStats;
  partitions: number | null;
  queues: IPartitionQueueStats[] | null;
}
export interface IDrainerStats {
  [key: string]: number | undefined;
  "last-proc-since"?: number;
  "last-rec-since"?: number;
}

export class PartitionDrainer {
  public raw: ConsumerGroup;
  public DRAIN_INTV: number = DEFAULT_DRAIN_INTV;
  public disablePauseResume = false;

  private stats: IDrainerStats = {};
  private queueMap: { [key: string]: PartitionQueue | null} | null = null;
  private drainMap: { [key: string]: any } = {};
  private totalIncomingMessages: number = 0;
  private incomingSinceLastDrain: number = 0;
  private lastReceived: number = Date.now();
  private receivedFirst: boolean = false;
  private drainStart: number | null = null;
  private lastMessageHandlerRef: ( (...args: any[]) => void ) | null = null;
  private drainTargetTopic: string | null = null;
  private drainEvent?: DrainEventCallback;

  constructor(
    public consumer: Kafka,
    public asyncLimit: number = 1,
    public commitOnDrain: boolean = false,
    public autoJsonParsing: boolean = true,
  ) {
    if (!consumer || !(consumer instanceof Kafka) || !consumer.isConsumer) {
      throw new Error("consumer is not a valid Sinek Kafka(Consumer)");
    }
    if (!consumer.consumer) {
      throw new Error("Consumer is not configured and initialized");
    }

    this.raw = consumer.consumer;
  }

  /**
   * stops any active drain process
   * closes the consumer and its client
   */
  public close(): Promise<any> | null {

    if (this.lastMessageHandlerRef) {
      this.raw.removeListener("message", this.lastMessageHandlerRef);
      this.lastMessageHandlerRef = null;
    } else {
      this._getLogger().warn("message handler ref not present during close, could not remove listener.");
    }

    this._resetQueueMaps();

    this._getLogger().info("[Drainer] closed.");
    return this.consumer.close();
  }

  /**
   * returns a few insights
   * @returns {{totalIncoming: number, last: (number|*), isPaused: *}}
   */
  public getStats(): IPartitionDrainerStats {
    return {
      lastMessage: this.lastReceived,
      totalIncoming: this.totalIncomingMessages,

      receivedFirstMsg: this.receivedFirst,

      isPaused: this.consumer && this.consumer.isConsumer ? this.isPaused() : null,

      drainStats: this.stats,
      partitions: this.queueMap ? Object.keys(this.queueMap!).length : null,
      queues: this.queueMap ?
        Object.keys(this.queueMap!).map((key: string) => this.queueMap![key].getStats()) :
        null,
    };
  }

  /**
   * resets all offsets and starts from being
   * also un-pauses consumer if necessary
   * @param topics
   * @returns {Promise.<TResult>}
   * @deprecated
   */
  public resetConsumer(): Promise<never> {
    return Promise.reject("resetConsumer has been removed, due to supporting bad kafka consumer behaviour.");
  }

  /**
   * resets all offsets and makes sure the consumer is paused
   * @param topics
   * @returns {Promise.<TResult>}
   * @deprecated
   */
  public resetOffset(): Promise<never> {
    return Promise.reject("resetOffset has been removed, due to supporting bad kafka consumer behaviour.");
  }

  /**
   * main reg. function, pass it a function to receive messages
   * under flow control, returns a promise
   * @param topic
   * @param drainEvent
   */
  public drain(topic: string = "t", drainEvent: DrainEventCallback) {

    this.drainTargetTopic = topic;

    if (!drainEvent || typeof drainEvent !== "function") {
      throw new Error("drainEvent must be a valid function");
    }

    if (this.drainEvent) {
      throw new Error("a drain process is currently active.");
    }

    this.incomingSinceLastDrain = this.totalIncomingMessages;
    this.drainEvent = drainEvent;
    this.lastReceived = Date.now();
    this.stats = {};

    return this._buildOffsetMap(topic, drainEvent, this.asyncLimit).then((maps) => {

      this.queueMap = maps.queueMap;
      this.drainMap = maps.drainMap;

      this._startToReceiveMessages();

      if (this.isPaused()) {
        this.resume();
      }
    });
  }

  /**
   * main req. function, pass it a function to receive messages
   * under flow control, until they are stall for a certain amount
   * of time (e.g. when all messages on the queue are consumed)
   * returns a Promise
   * @param topic
   * @param drainEvent
   * @param drainThreshold
   * @param timeout
   */
  public drainOnce(
    topic: string = "t",
    drainEvent: DrainEventCallback,
    drainThreshold: number = 10000,
    timeout: number = 0,
  ) {
    return new Promise((resolve, reject) => {

      if (!drainEvent || typeof drainEvent !== "function") {
        return reject("drainEvent must be a valid function");
      }

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

        const spanProcessed = Date.now() - this._getEarliestProcessedOnQueues();
        const spanReceived = Date.now() - this.lastReceived;

        this._getLogger().debug("drainOnce interval running, current span-rec: " +
          `${spanReceived} / span-proc: ${spanProcessed} ms.`);

        // set stats
        this._countStats("intv-cycle");
        this.stats["last-proc-since"] = spanProcessed;
        this.stats["last-rec-since"] = spanReceived;

        // choose the smaller span
        const span = spanProcessed < spanReceived ? spanProcessed : spanReceived;

        if (span >= drainThreshold) {
          this._getLogger().info(`drainOnce span ${span} hit threshold ${drainThreshold}.`);
          clearInterval(intv);
          clearTimeout(t);
          this.stopDrain();
          resolve(this.totalIncomingMessages - this.incomingSinceLastDrain);
        }
      }, this.DRAIN_INTV);

      if (timeout !== 0) {
        this._getLogger().info(`drainOnce timeout active: ${timeout} ms.`);
        t = setTimeout(() => {
          this._getLogger().warn(`drainOnce timeout hit after ${timeout} ms.`);
          clearInterval(intv);
          this.stopDrain();
          reject("drainOnce ran into timeout.");
        }, timeout);
      }

      // start the drain process
      this.drain(topic, drainEvent).then(() => {
        this._getLogger().info("drain process of drainOnce has started.");
      }).catch ((e: Error) => {
        reject(`failed to start drain process of drainOnce, because: ${e}.`);
      });
    });
  }

  /**
   * stops any active drain process
   */
  public stopDrain() {

    if (!this.drainEvent) {
      throw new Error("there is no drain active.");
    }

    this.drainTargetTopic = null;

    // reset
    if (this.lastMessageHandlerRef) {
      this.raw.removeListener("message", this.lastMessageHandlerRef);
      this.lastMessageHandlerRef = null;
    } else {
      this._getLogger().warn("message handler ref not present during close, could not remove listener.");
    }

    this.drainEvent = undefined;
    this.receivedFirst = false;

    this._resetQueueMaps();

    const duration = (Date.now() - this.drainStart!) / 1000;
    this._getLogger().info(`[Drainer] stopped drain process, had been open for ${duration} seconds.`);
  }

  /**
   * removes kafka topics (if broker allows this action)
   * @param topics
   */
  public removeTopics(topics = []) {
    return new Promise((resolve, reject) => {
      this._getLogger().info(`deleting topics ${JSON.stringify(topics)}.`);
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
      this._countStats("paused");
    }

    return this.consumer.pause();
  }

  public resume() {

    if (this.isPaused()) {
      this._countStats("resumed");
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

  private _startToReceiveMessages() {
    this.lastMessageHandlerRef = this._onMessage.bind(this);
    this.raw.on("message", this.lastMessageHandlerRef);
    this._getLogger().info("[Drainer] started drain process.");
    this.drainStart = Date.now();
  }

  private _onMessage(message) { // TODO: correct interface for message

    this._getLogger().debug("received kafka message => length: " + message.value.length + ", offset: " +
      message.offset + ", partition: " + message.partition + ", on topic: " + message.topic);

    if (this.autoJsonParsing) {
      try {
        message.value = JSON.parse(message.value);
      } catch (e) {
        this._countStats("msg-parse-fail");
        return this.emit("error", "failed to json parse message value: " + message);
      }

      if (!message.value) {
        this._countStats("msg-empty");
        return this.emit("error", "message value is empty: " + message);
      }
    }

    // we only want to drain messages that belong to our topic
    if (message.topic === this.drainTargetTopic) {

      // identify queue for this topic
      if (!this.queueMap) {
        this._countStats("queue-map-missing");
        this._getLogger().warn("received message, but queue map is missing.");
      } else if (!this.queueMap[message.partition.toString()]) {
        this._countStats("queue-partition-missing");
        this._getLogger().warn("received message, but queue partition is missing for partition: " + message.partition);
      } else {
        // and push message into queue
        this.queueMap![message.partition.toString()]!.push(message);
      }
    } else {
      this._getLogger().warn(
        /* tslint:disable:max-line-length */
        `receiving messages from other topic ${ message.topic } only expecting to receive from ${this.drainTargetTopic} for this instance.`,
        /* tslint:enable */
        );
    }

    if (!this.disablePauseResume) {
      this.pause();
    }

    this.totalIncomingMessages++;
    this.lastReceived = Date.now();

    if (!this.receivedFirst) {
      this.receivedFirst = true;
      this._getLogger().info("consumer received first message.");
      this.emit("first-drain-message", message);
    }
  }

  private _countStats(key: string) {

    if (!this.stats) {
      return;
    }

    if (!this.stats[key]) {
      this.stats[key] = 1;
      return;
    }

    this.stats[key]!++;
  }

  private _getLogger() {
    return (this.consumer as any).getLogger(); // Private member access!
  }

  /**
   * gets all partitions of the given topic
   * and builds a map of async.queues (PartionQueue)
   * with a single queue for each partition
   * they will all call the same queue-drain callback to control the message flow
   * and they will all call the same drain-event callback to process messages
   * queues also expose their own stats
   * @param topic
   * @param drainEvent
   * @param asyncLimit
   * @returns {*}
   * @private
   */
  private _buildOffsetMap(
    topic: string,
    drainEvent: DrainEventCallback,
    asyncLimit: number = 1,
  ): Promise<{ drainMap: { [key: number]: PartitionQueue }, queueMap: { [key: number]: boolean } }> {

    if (typeof topic !== "string") {
      return Promise.reject("offset map can only be build for a single topic.");
    }

    if (typeof drainEvent !== "function") {
      return Promise.reject("drainEvent must be a valid function.");
    }

    if (this.consumer.getTopics().indexOf(topic) === -1) {
      return Promise.reject(topic + " is not a supported topic, it has to be set during becomeConsumer().");
    }

    return this.consumer.getPartitions(topic).then((partitions: number[]) => {

      if (!partitions || partitions.length <= 0) {
        return Promise.reject(`partitions request for topic ${topic} returned empty.`);
      }

      const queueMap: { [key: number]: PartitionQueue } = {};
      const drainMap: { [key: number]: boolean } = {};

      partitions.forEach((partition) => {
        // build a parition queue for each partition
        queueMap[partition] = new PartitionQueue(
          partition,
          drainEvent,
          this,
          asyncLimit,
          this._onPartitionQueueDrain.bind(this),
        ).build();

        // drain map is build to check if all queues have been drained
        drainMap[partition] = false;
      });

      return {
        drainMap,
        queueMap,
      };
    });
  }

  /**
   * partiton queue drain callback that makes sure to resume the consumer if
   * all queues have drained
   * @param partition
   * @param offset
   * @private
   */
  private _onPartitionQueueDrain(partition: number) {

    if (typeof this.drainMap[partition] === "undefined") {
      this._getLogger().warn(`partition queue drain called but ${partition} is not a present key.`);
      return;
    }

    this.drainMap[partition] = true;

    // this queue drained, lets commit the latest offset
    // Private member access!
    if (this.commitOnDrain && ((this.consumer as any).isManual || !(this.consumer as any).autoCommitEnabled)) {

      if ((this.consumer as any).autoCommitEnabled) { // Private member access!
        throw new Error("you have started a consumer with auto commit enabled, but requested partition drainer" +
          "to run commits manually for you - both cannot work at the same time.");
      }

      // running setConsumerOffset while commit manually is a bad idea
      // message offset is already hold in the client, only committing is needed
      // this.consumer.setConsumerOffset(this.drainTargetTopic, partition, offset);
    }

    if (
      Object.keys(this.drainMap)
        .map((key: string) => this.drainMap[parseInt(key, 10)])
        .filter((v: boolean) => !v)
        .length
    ) {
      this._getLogger().debug("not all partition queues have drained yet.");
    } else {
      this._getLogger().debug("all partition queues have drained.");

      // reset drain map
      Object.keys(this.drainMap).forEach((key: string) => {
        this.drainMap[key] = false;
      });

      if (!this.commitOnDrain) {
        if (!this.disablePauseResume) {
          this.resume();
        }
        return; // do not execute commit logic^
      }

      // resume consumer, which will cause new message to be pushed into the queues
      // but make sure to commit current offsets first
      this.consumer.commitCurrentOffsets().then(() => {
        if (!this.disablePauseResume) {
          this.resume();
        }
      }).catch ((e: Error) => {
        this._getLogger().error(`failed to commit offsets after all partitions have been drained. ${e}.`);
        if (!this.disablePauseResume) {
          this.resume();
        }
      });
    }
  }

  private _resetQueueMaps(): void {

    this._getLogger().info("resetting queue maps.");

    if (this.queueMap) {
      Object.keys(this.queueMap).forEach((key: string) => {
        this.queueMap[key].close();
      });
      this.queueMap = null;
      this.drainMap = {};
    }
  }

  private _getEarliestProcessedOnQueues(): number {

    // error prevention
    if (!this.queueMap) {
      return this.lastReceived;
    }

    let earliest = this.queueMap[Object.keys(this.queueMap)[0]].getLastProcessed();
    let ne = null;
    Object.keys(this.queueMap).forEach(key => {
      ne = this.queueMap[key].getLastProcessed();
      if (ne < earliest) {
        earliest = ne;
      }
    });

    return earliest;
  }

}
