"use strict";

import Promise from "bluebird";
import {HighLevelProducer, KeyedMessage, ProduceRequest} from "kafka-node";
import { v3 as murmur } from "murmurhash";
import { v4 as uuid } from "uuid";
import { Kafka } from "./Kafka";

import { CompressionTypes } from "../tools";

export const enum MESSAGE_TYPES {
  PUBLISH = "-published",
  UNPUBLISH = "-unpublished",
  UPDATE = "-updated",
}

// TODO: proof for optional properties
export interface IMessage {
  id?: string;
  attributes?: number;
  key: string | null;
  messages: string[];
  partition: number;
  time?: string;
  topic: string;
  type?: string;
  payload?: IKeyedPayload;
}

export interface IPublisherStats {
  isPaused: boolean | null;
  last: number;
  totalPublished: number;
}

export interface IKeyedPayload {
  attributes: number;
  messages: KeyedMessage;
  key?: string | null;
  partition: number;
  topic: string;
}

export interface IKafkaPayload {
  id?: string;
  version?: number;
}

export class Publisher {
  /**
   * returns a default message type object
   * @returns {{topic: string, messages: Array, key: null, partition: number, attributes: number}}
   */
  public static getKafkaBaseMessage(): IMessage {
    return {
      attributes: 0,
      key: null,
      messages: [],
      partition: 0,
      topic: "",
    };
  }

  public raw: HighLevelProducer;
  public readonly CompressionTypes = CompressionTypes;

  private lastProcessed: number = Date.now();
  private totalSentMessages: number = 0;
  private paused: boolean = false;
  private buffer: { [key: string]: IKeyedPayload[] } = {};
  private flushIntv?: NodeJS.Timer;
  private bufferDisabled: boolean = false;

  constructor(
    public producer: Kafka,
    public partitionCount = 1,
    public autoFlushBuffer = 0,
    public flushPeriod = 100,
  ) {

    if (!producer || !(producer instanceof Kafka) || !producer.isProducer) {
      throw new Error("producer is not a valid Sinek Kafka(Producer)");
    }
    if (!producer.producer) {
      throw new Error("producer is not configured or initialized correctly (raw producer is missing)");
    }

    this.raw = producer.producer as HighLevelProducer;

    this.disableBuffer();
  }

  /**
   * default behaviour
   */
  public disableBuffer(): void {
    this.getLogger().info("[Publisher] buffer disabled.");
    this.stopAutoBufferFlushInterval();
    this.bufferDisabled = true;
  }

  /**
   * BETA
   */
  public enableBuffer(): void {

    this.getLogger().info("[Publisher] buffer enabled.");

    if (this.autoFlushBuffer > 0) {
      this.setAutoFlushBuffer(this.autoFlushBuffer, this.flushPeriod);
    }
  }

  public setAutoFlushBuffer(minBufferSize = 0, period = 100): void {

    if (typeof minBufferSize !== "number" || minBufferSize < 0) {
      throw new Error("minBufferSize must be a number and higher or equal to 0.");
    }

    if (typeof period !== "number" || period < 5 || period > 60000) {
      throw new Error("period must be a number and > 5 and < 60000.");
    }

    this.getLogger().info(`[Publisher] Adjusting auto flush buffer size: ${minBufferSize} and period: ${period}.`);
    this.runAutoBufferFlushInterval(minBufferSize, period);
  }

  public stopAutoFlushBuffer(): void {
    this.stopAutoBufferFlushInterval();
  }

  /**
   * closes the publisher (and the underlying producer/client)
   */
  public close(): Promise<any> | null {
    this.getLogger().info("[Publisher] closed.");
    this.stopAutoBufferFlushInterval();
    return this.producer.close();
  }

  /**
   * returns a few insights
   * @returns {{totalPublished: (number|*), last: (number|*), isPaused: *}}
   */
  public getStats(): IPublisherStats {
    return {
      isPaused: this.producer && this.producer.isProducer ? this.isPaused() : null,
      last: this.lastProcessed,
      totalPublished: this.totalSentMessages,
    };
  }

  /**
   * uses the partition count to identify
   * a partition in range using a hashed representation
   * of the key's string value
   * @param key
   * @param partitionCount
   * @returns {Promise}
   */
  public getPartitionForKey(key: string, partitionCount: number = 0): Promise<number> {

    if (typeof key !== "string") {
      return Promise.reject("key must be a valid string");
    }

    if (partitionCount === 0) {
      partitionCount = this.partitionCount;
    }

    return Promise.resolve(murmur(key) % partitionCount);
  }

  public getRandomPartition(partitionCount: number = 0): Promise<number> {
    return this.getPartitionForKey(uuid(), partitionCount);
  }

  /**
   * create topics (be aware that this requires
   * certain settings in your broker to be active)
   * @param topics
   */
  public createTopics(topics: string[] = ["t"]): Promise<any> {
    return new Promise((resolve, reject) => {
      this.getLogger().info(`[Publisher] creating topics ${JSON.stringify(topics)}.`);
      this.raw.createTopics(topics, true, (err, data) => {

        if (err) {
          return reject(err);
        }

        resolve(data);
      });
    });
  }

  /**
   * returns a kafka producer payload ready to be sent
   * identifies partition of message by using identifier
   * @param topic
   * @param identifier
   * @param object
   * @param compressionType
   * @param {string | null} partitionKey base string for partition determination
   * @returns {*}
   */
  public getKeyedPayload(
    topic: string = "t",
    identifier: string = "",
    object = {},
    compressionType: number = 0,
    partitionKey: string | null = null,
  ): Promise<IKeyedPayload> {

    if (!this.CompressionTypes.isValid(compressionType)) {
      return Promise.reject("compressionType is not valid checkout publisher.CompressionTypes.");
    }

    partitionKey = typeof partitionKey === "string" ? partitionKey : identifier;

    return this.getPartitionForKey(partitionKey).then((partition) => {
      return {
        attributes: compressionType,
        messages: new KeyedMessage(identifier, JSON.stringify(object)),
        partition,
        topic,
      };
    });
  }

  /**
   * easy access to compliant kafka topic api
   * this will create a store a message payload describing a "CREATE" event
   * @param topic
   * @param identifier
   * @param payload
   * @param version
   * @param compressionType
   * @param {string | null} partitionKey base string for partition determination
   * @returns {*}
   */
  public bufferPublishMessage(
    topic: string,
    identifier: string,
    payload: IKafkaPayload,
    version: number = 1,
    compressionType: number = 0,
    partitionKey: number | null = null,
  ): Promise<IKeyedPayload> {

    if (typeof identifier !== "string") {
      return Promise.reject("expecting identifier to be of type string.");
    }

    if (typeof payload !== "object") {
      return Promise.reject("expecting object to be of type object.");
    }

    payload = {
      id: identifier,
      version,
      ...payload,
    };

    return this.appendBuffer(topic, identifier, {
      id: uuid(),
      key: identifier,
      payload,
      time: (new Date()).toISOString(),
      type: topic + MESSAGE_TYPES.PUBLISH,
    }, compressionType, partitionKey);
  }

  /**
   * easy access to compliant kafka topic api
   * this will create a store a message payload describing a "DELETE" event
   * @param topic
   * @param identifier
   * @param object
   * @param version
   * @param compressionType
   * @param partitionKey
   * @returns {*}
   */
  public bufferUnpublishMessage(
    topic: string,
    identifier: string,
    payload: IKafkaPayload = {},
    version: number = 1,
    compressionType: number = 0,
    partitionKey: number | null = null,
  ) {

    if (typeof identifier !== "string") {
      return Promise.reject("expecting identifier to be of type string.");
    }

    if (typeof payload !== "object") {
      return Promise.reject("expecting object to be of type object.");
    }

    payload = {
      id: identifier,
      version,
      ...payload,
    };

    return this.appendBuffer(topic, identifier, {
      id: uuid(),
      key: identifier,
      payload,
      time: (new Date()).toISOString(),
      type: topic + MESSAGE_TYPES.UNPUBLISH,
    }, compressionType, partitionKey);
  }

  /**
   * easy access to compliant kafka topic api
   * this will create a store a message payload describing an "UPDATE" event
   * @param topic
   * @param identifier
   * @param payload
   * @param version
   * @param compressionType
   * @param partitionKey
   * @returns {*}
   */
  public bufferUpdateMessage(
    topic: string,
    identifier: string,
    payload: IKafkaPayload,
    version: number = 1,
    compressionType: number = 0,
    partitionKey: number | null = null,
  ) {

    if (typeof identifier !== "string") {
      return Promise.reject("expecting identifier to be of type string.");
    }

    if (typeof payload !== "object") {
      return Promise.reject("expecting object to be of type object.");
    }

    payload = {
      id: identifier,
      version,
      ...payload,
    };

    return this.appendBuffer(topic, identifier, {
      id: uuid(),
      key: identifier,
      payload,
      time: (new Date()).toISOString(),
      type: topic + MESSAGE_TYPES.UPDATE,
    }, compressionType, partitionKey);
  }

  /**
   * build a buffer per topic for message payloads
   * if autoBufferFlush is > 0 flushBuffer might be called
   * @param topic
   * @param identifier
   * @param object
   * @param compressionType
   * @param {string | null} partitionKey base string for partition determination
   * @returns {Promise.<TResult>}
   */
  public appendBuffer(
    topic: string,
    identifier: string,
    object: IMessage,
    compressionType: number = 0,
    partitionKey: string | null = null,
  ): Promise<void> {

    return this.getKeyedPayload(topic, identifier, object, compressionType, partitionKey).then((payload) => {

      // if buffer is disbaled, this message will be send instantly
      if (this.bufferDisabled) {
        return this.batch([payload]);
      }

      if (!this.buffer[topic]) {
        this.buffer[topic] = [];
      }

      this.buffer[topic].push(payload);
    });
  }

  /**
   * send all message payloads in buffer for a topic
   * in a single batch request
   * @param topic
   * @param skipBlock
   * @returns {*}
   */
  public flushBuffer(topic: string) {
    if (!this.buffer[topic]) {
      return Promise.reject(`topic ${topic} has no buffer, you should call appendBuffer() first.`);
    }

    const batch = this.buffer[topic];
    this.buffer[topic] = [];

    return this.batch(batch);
  }

  /**
   * appends and sends the message payloads in the buffer
   * (you can also use this so send a single message immediately)
   * @param topic
   * @param identifier
   * @param object
   * @param compressionType
   * @returns {Promise.<TResult>}
   */
  public appendAndFlushBuffer(
    topic: string,
    identifier: string,
    object: IKafkaPayload,
    compressionType: number = 0,
  ): Promise<any> {
    return this.appendBuffer(topic, identifier, object, compressionType).then(() => {
      return this.flushBuffer(topic);
    });
  }

  /**
   * most versatile function to produce a message on a topic(s)
   * you can send multiple messages at once (but keep them to the same topic!)
   * if you need full flexibility on payload (message definition) basis
   * you should use .batch([])
   * @param topic
   * @param messages
   * @param partitionKey
   * @param partition
   * @param compressionType
   * @returns {*}
   */
  public send(
    topic: string = "t",
    messages: string[] = [],
    partitionKey: string | null = null,
    partition?: number,
    compressionType: number = 0,
  ) {
    if (!this.CompressionTypes.isValid(compressionType)) {
      return Promise.reject("compressionType is not valid checkout publisher.CompressionTypes.");
    }

    const payload: ProduceRequest = {
      attributes: compressionType,
      key: partitionKey,
      messages,
      partition,
      topic,
    };

    return this.batch([ payload ]);
  }

  /**
   * leaves full flexibility when sending different message definitions (e.g. mulitple topics)
   * at once use with care: https://www.npmjs.com/package/kafka-node#sendpayloads-cb
   * @param payloads
   * @returns {Promise.<{}>}
   */
  public batch(payloads: ProduceRequest[]): Promise<any> {

    if (this.paused) {
      return Promise.resolve({});
    }

    return new Promise((resolve, reject) => {
      this.raw.send(payloads, (err, data) => {

        if (err) {
          return reject(err);
        }

        // update stats
        this.lastProcessed = Date.now();
        payloads.forEach((p) => {
          if (p && p.messages) {
            if (Array.isArray(p.messages)) {
              this.totalSentMessages += p.messages.length;
            } else {
              this.totalSentMessages++;
            }
          }
        });

        resolve(data);
      });
    });
  }

  public pause(): void {
    this.paused = true;
  }

  public resume(): void {
    this.paused = false;
  }

  public isPaused(): boolean {
    return this.paused;
  }

  public refreshMetadata(topics = []): Promise<void> {
    return this.producer.refreshMetadata(topics);
  }

  /**
   * producer proxy
   */
  public on(event: string | symbol, listener: (...args: any[]) => void): this {
    this.producer.on(event, listener);
    return this;
  }

  /**
   * producer proxy
   */
  public once(event: string | symbol, listener: (...args: any[]) => void): this {
    this.producer.once(event, listener);
    return this;
  }

  /**
   * producer proxy
   */
  public removeListener(event: string | symbol, listener: (...args: any[]) => void): this {
    this.producer.removeListener(event, listener);
    return this;
  }

  /**
   * producer proxy
   */
  public emit(event: string | symbol, ...args: any[]) {
    this.producer.emit(event, ...args);
  }

  private runAutoBufferFlushInterval(minSize: number, ms: number) {
    this.flushIntv = setInterval(() => {

      Promise.all(Object
        .keys(this.buffer)
        .filter((k: string) => this.buffer[k].length >= minSize)
        .map((topic) => this.flushBuffer(topic)))
        .then(() => {
          this.getLogger().debug("[Publisher] flushed buffer.");
        }, (e: Error) => {
          this.getLogger().error(`[Publisher] failed to flush buffer: ${e}.`);
        });
    }, ms);
  }

  private stopAutoBufferFlushInterval() {

    if (this.flushIntv) {
      this.getLogger().debug("[Publisher] stopping auto-buffer flush interval.");
      clearInterval(this.flushIntv);
    }
  }

  private getLogger() {
    return (this.producer as any).getLogger(); // access private member
  }
}
