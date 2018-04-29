import Promise from "bluebird";
import debug from "debug";
import { EventEmitter } from "events";
import {
  Client,
  ConsumerGroup,
  ConsumerGroupOptions,
  HighLevelProducer,
  KafkaClient,
  KafkaClientOptions,
  Offset,
} from "kafka-node";
import { ILogger } from "../common";

const NOOPL: ILogger = {
  debug: debug("sinek:debug"),
  error: debug("sinek:error"),
  info: debug("sinek:info"),
  warn: debug("sinek:warn"),
};

const DEFAULT_RETRY_OPTIONS = {
  factor: 3,
  forever: true,
  maxTimeout: 30000, // 30 secs
  minTimeout: 1000, // 1 sec
  randomize: true,
  retries: 1000, // overwritten by forever
  unref: false,
};

export class Kafka extends EventEmitter {

  public client: Client | null = null;

  // consumer
  public consumer: ConsumerGroup | null = null;
  public offset = null;
  public isConsumer = false;

  // producer
  public isProducer: boolean = false;
  public producer: HighLevelProducer | null = null;
  public targetTopics: string[] = [];

  // private declarations

  // consumer
  private autoCommitEnabled?: boolean;
  private isManual: boolean = false;
  // producer
  private producerReadyFired: boolean = false;

  // common

  constructor(
    public conString: string,
    private logger: ILogger | null = null,
    public connectDirectlyToBroker: boolean = false,
  ) {
    super();
  }

  public getPartitions(topic: string): Promise<any> {
    return new Promise((resolve, reject) => {

      if (!this.client) {
        return reject("client is not defined yet, cannot create offset to gather partitions.");
      }

      const offset = new Offset(this.client);
      offset.fetchEarliestOffsets([topic], (err, data) => {
        if (err || !data[topic]) {
          return reject("failed to get offsets of topic: " + topic + "; " + err);
        }

        resolve(Object.keys(data[topic]).map((key) => key));
      });
    });
  }

  public getEarliestOffsets(topic: string): Promise<any> {
    return new Promise((resolve, reject) => {

      if (!this.client) {
        return reject("client is not defined yet, cannot create offset to reset.");
      }

      const offset = new Offset(this.client);
      offset.fetchEarliestOffsets([topic], (err, data) => {

        if (err || !data[topic]) {
          return reject("failed to get offsets of topic: " + topic + "; " + err);
        }

        resolve(data[topic]);
      });
    });
  }

  public getOffsets(topic: string): Promise<any> {
    return new Promise((resolve, reject) => {

      if (!this.client) {
        return reject("client is not defined yet, cannot create offset to reset.");
      }

      const offset = new Offset(this.client);
      offset.fetchLatestOffsets([topic], (err, data) => {

        if (err || !data[topic]) {
          return reject("failed to get offsets of topic: " + topic + "; " + err);
        }

        resolve(data[topic]);
      });
    });
  }

  public getTopics(): string[] {
    return this.targetTopics;
  }

  public setConsumerOffset(topic: string = "t", partition: number = 0, offset: number = 0): void {
    this._getLogger().debug("adjusting offset for topic: " + topic + " on partition: " + partition + " to " + offset);
    if (this.consumer) {
      this.consumer.setOffset(topic, partition, offset);
    }
  }

  public commitCurrentOffsets(): Promise<any> {
    return new Promise((resolve, reject) => {
      if (this.consumer) {
        return this.consumer.commit((err, data) => {
          if (err) {
            return reject(err);
          }

          resolve(data);
        });
      }
      reject(new Error("Your consumer is not initialized"));
    });
  }

  public becomeManualConsumer(
    topics?: string[],
    groupId?: string,
    options?: ConsumerGroupOptions,
    dontListenForSIGINT?: boolean,
  ): void {
    this.isManual = true;
    return this.becomeConsumer(topics, groupId, options, dontListenForSIGINT, false);
  }

  public becomeConsumer(
    topics = ["t"],
    groupId = "kafka-node-group",
    options?: ConsumerGroupOptions,
    dontListenForSIGINT = false,
    autoCommit = true,
  ): void {

    if (!Array.isArray(topics) || topics.length <= 0) {
      throw new Error("becomeConsumer requires a valid topics array, with at least a single topic.");
    }

    if (this.isConsumer) {
      throw new Error("this kafka instance has already been initialised as consumer.");
    }

    if (this.isProducer) {
      throw new Error("this kafka instance has already been initialised as producer.");
    }

    if (!groupId) {
      throw new Error("missing groupId or consumer configuration.");
    }

    if (!options) {
      options = { groupId };
    }

    options = {
      autoCommit,
      autoCommitIntervalMs: 5000,
      connectRetryOptions: this.connectDirectlyToBroker ? DEFAULT_RETRY_OPTIONS : undefined,
      fetchMaxBytes: 1024 * 100,
      fetchMaxWaitMs: 100,
      fetchMinBytes: 1,
      fromOffset: "earliest", // latest
      groupId,
      host: this.connectDirectlyToBroker ? undefined : this.conString,
      kafkaHost: this.connectDirectlyToBroker ? this.conString : undefined,
      migrateHLC: false,
      migrateRolling: false,
      protocol: ["roundrobin"],
      sessionTimeout: 30000,
      ssl: false,

      // zk: undefined,
      // batch: undefined,

      ...options,
    };

    this.autoCommitEnabled = options!.autoCommit;

    this.consumer = new ConsumerGroup(options!, topics);
    this.client = this.consumer.client;
    this.isConsumer = true;
    this.pause();

    this.targetTopics = topics;
    this._getLogger().info("starting ConsumerGroup for topic: " + JSON.stringify(topics));

    this._attachConsumerListeners(dontListenForSIGINT);
  }

  public becomeProducer(
    targetTopics: string[] = ["t"],
    clientId: string = "kafka-node-client",
    options: KafkaClientOptions = {},
  ): void {

    if (this.isConsumer) {
      throw new Error("this kafka instance has already been initialised as consumer.");
    }

    if (this.isProducer) {
      throw new Error("this kafka instance has already been initialised as producer.");
    }

    options = {
      ackTimeoutMs: 100,
      partitionerType: 3,
      requireAcks: 1,
      ...options,
    };

    this.client = null;
    if (this.connectDirectlyToBroker) {

      const kafkaOptions: KafkaClientOptions = {
        autoConnect: options.autoConnect || true, // TODO: makes it sense???
        connectRetryOptions: DEFAULT_RETRY_OPTIONS,
        connectTimeout: 1000,
        kafkaHost: this.conString,
        requestTimeout: 30000,
        ssl: !!options.sslOptions,
        sslOptions: options.sslOptions,
      };

      this.client = new KafkaClient(kafkaOptions);
    } else {
      this.client = new Client(this.conString, clientId, {}, options.sslOptions || {});
    }

    // TODO: producer options vs. client options!
    this.producer = new HighLevelProducer(this.client, _options);
    this.isProducer = true;

    this._getLogger().info("starting Producer.");
    this.targetTopics = targetTopics;
    this._attachProducerListeners();
  }

  /**
   * @deprecated
   */
  public hardOffsetReset(): Promise<never> {
    return Promise.reject("hardOffsetReset has been removed, as it was supporting bad kafka consumer behaviour.");
  }

  public refreshMetadata(topics: string[] = []): Promise<void> {

    if (!topics || topics.length <= 0) {
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      if (!this.client) {
        reject(new Error("No client initialized"));
      }
      this.client!.refreshMetadata(topics, () => {
        this._getLogger().info("meta-data refreshed.");
        resolve();
      });
    });
  }

  public isPaused(): boolean {

    if (this.isConsumer) {
      return this.consumer!.paused;
    }

    return false;
  }

  public pause(): boolean {

    if (this.isConsumer) {
      this.consumer!.pause();
      return true;
    }

    return false;
  }

  public resume(): boolean {

    if (this.isConsumer) {
      this.consumer!.resume();
      return true;
    }

    return false;
  }

  public close(commit = false): Promise<any> | null {

    if (this.isConsumer) {
      return this._closeConsumer(commit);
    }

    if (this.isProducer) {
      return this._closeProducer();
    }

    return null;
  }

  private _getLogger(): ILogger {

    if (this.logger) {
      return this.logger;
    }

    return NOOPL;
  }

  private _attachProducerListeners(): void {

    this.client!.on("connect", () => {
      this._getLogger().info("producer is connected.");
    });

    this.producer!.on("ready", () => {

      this._getLogger().debug("producer ready fired.");
      if (this.producerReadyFired) {
        return;
      }

      this.producerReadyFired = true;
      this._getLogger().info("producer is ready.");

      // prevents key-partition errors
      this.refreshMetadata(this.targetTopics).then(() => {
        this.emit("ready");
      });
    });

    this.producer!.on("error", (error: Error) => {
      // dont log these, they emit very often
      this.emit("error", error);
    });
  }

  private _attachConsumerListeners(dontListenForSIGINT = false, commitOnSIGINT = false): void {

    this.consumer!.once("connect", () => {
      this._getLogger().info("consumer is connected / ready.");
      this.emit("connect");
      this.emit("ready");
    });

    // do not listen for "message" here

    this.consumer!.on("error", (error: Error) => {
      // don't log these, they emit very often
      this.emit("error", error);
    });

    this.consumer!.on("offsetOutOfRange", (error: Error) => {
      // don't log these, they emit very often
      this.emit("error", error);
    });

    // prevents re-balance errors
    if (!dontListenForSIGINT) {
      process.on("SIGINT", () => {
        if (this.consumer) {
          this.consumer.close(commitOnSIGINT, () => {
            process.exit();
          });
        }
      });
    }
  }

  private _resetConsumer(): void {
    this.isConsumer = false;
    this.client = null;
    this.consumer = null;
  }

  private _resetProducer(): void {
    this.isProducer = false;
    this.client = null;
    this.producer = null;
    this.producerReadyFired = false;
  }

  private _closeConsumer(commit: boolean): Promise<any> {
    return new Promise((resolve, reject) => {

      this._getLogger().info("trying to close consumer.");

      if (!this.consumer) {
        return reject("consumer is null");
      }

      if (!commit) {

        this.consumer.close(() => {
          this._resetConsumer();
          resolve();
        });

        return;
      }

      this._getLogger().info("trying to commit kafka consumer before close.");

      this.consumer.commit((err, data) => {

        if (err) {
          return reject(err);
        }

        // TODO: TypeScript gives us the hint that this.consumer can possibly be null!
        this.consumer!.close(() => {
          this._resetConsumer();
          resolve(data);
        });
      });
    });
  }

  private _closeProducer(): Promise<boolean> {
    return new Promise((resolve, reject) => {

      this._getLogger().info("trying to close producer.");

      if (!this.producer) {
        return reject("producer is null");
      }

      this.producer.close(() => {
        this._resetProducer();
        resolve(true);
      });
    });
  }
}
