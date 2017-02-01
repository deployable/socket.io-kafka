/**
 * Kafka adapter for socket.io
 *
 * @example <caption>Example usage</caption>
 * var io = require('socket.io')(3000);
 * var kafka = require('socket.io-kafka');
 * io.adapter(kafka('localhost:2181'));
 *
 * @module socket.io-kafka
 * @see {@link https://www.npmjs.com/package/kafka-node|kafka-node}
 *
 * @author Guilherme Hermeto
 * @licence {@link http://opensource.org/licenses/MIT|MIT}
 */

const debug = require('debug')('socket.io-kafka:index')
const Promise = require('bluebird')
const kafka = require('kafka-node')
const Adapter = require('socket.io-adapter')
const uuid = require('uuid')

/**
 * Generator for the kafka Adapater
 *
 * @param {string} optional, zookeeper connection string
 * @param {object} adapter options
 * @return {Kafka} adapter
 * @api public
 */
function adapter(uri, options = {}) {
  if (!options.key) options.key = 'socket.io-kafka-group'
  let client

  // handle options only
  if (typeof uri === 'object') {
    options = uri
    if ( options.uri ) uri = options.uri
    else {
      if ( options.host && options.port ) `${options.host}:${options.port}`
      else throw new Error('URI or host/port are required.')
    }
  }
  clientId = options.clientId || 'socket.io-kafka'

  // create producer and consumer if they weren't provided
  if (!options.producer || !options.consumer) {
      debug('creating new kafka client')
      
      //client = new kafka.Client(uri, clientId, { retries: 4 })
      client = new kafka.Client()
      
      client.on('error', function (err, data) {
        console.error('error', err, data)
      })

      if (!options.producer) {
        debug('creating new kafka producer')
        options.producer = new kafka.Producer(client)
        //debug('created producer',options.producer)
        options.producer = Promise.promisifyAll(options.producer)
      }

      if (!options.consumer) {
        debug('creating new kafka consumer')
        options.consumer = new kafka.Consumer(client, [], { groupId: options.key })
        //debug('created consumer',options.consumer)
        options.consumer = Promise.promisifyAll(options.consumer)
      }
  }

  /**
   * Kafka Adapter constructor.
   *
   * @constructor
   * @param {object} channel namespace
   * @api public
   */

  class Kafka extends Adapter {

    constructor( nsp ){
      super(nsp)
      let create = options.createTopics

      Adapter.call(this, nsp)

      this.uid = uuid.v4()
      this.options = options
      this.prefix = options.key
      this.consumer = options.consumer
      this.producer = options.producer
      this.mainTopic = this.prefix + nsp.name
      options.createTopics = (create === undefined) ? true : create

      options.producer.on('ready', () => {
        debug('producer ready')
        this.createTopic(this.mainTopic)
        this.subscribe(this.mainTopic)

        // handle incoming messages to the channel
        this.consumer.on('message', this.onMessage.bind(this))
        this.consumer.on('error', this.onError.bind(this))
      })
    }

    /**
     * Emits the error.
     *
     * @param {object|string} error
     * @api private
     */
    onError (...errors) {
      debug('emitting errors', errors)
      errors.forEach(error => this.emit('error', error))
    }

    /**
     * Process a message received by a consumer. Ignores messages which come
     * from the same process.
     *
     * @param {object} kafka message
     * @api private
     */
    onMessage (kafkaMessage) {
      let message, packet

      try {
        debug('kafkaMessage', kafkaMessage)
        if ( !kafkaMessage ) throw new Error('No message')
        if ( !kafkaMessage.value ) throw new Error('No message.value')
        message = JSON.parse(kafkaMessage.value)
        if ( this.uid === message[0] ) return debug('ignore same uid')
        packet = message[1]

        if ( !packet ) return debug('ignore bad packet')
        if ( packet.nsp === undefined ) packet.nsp = '/'
        if ( packet.nsp !== this.nsp.name ) return debug('ignore different namespace')

        this.broadcast(packet, message[2], true)

      } catch (error) {
        // failed to parse JSON?
        this.onError(error)
      }
    }

    /**
     * Converts a socket.io channel into a safe kafka topic name.
     *
     * @param {string} cahnnel name
     * @return {string} topic name
     * @api private
     */
    safeTopicName (channel) {
      return channel.replace('/', '_')
    }

    /**
     * Uses the producer to create a new topic synchronously if
     * options.createTopics is true.
     *
     * @param {string} topic to create
     * @api private
     */
    createTopic (channel) {
      let chn = this.safeTopicName(channel)

      debug('creating topic %s', chn)
      if (this.options.createTopics) {
        //this.producer.createTopics(chn, this.onError.bind(this));
        this.producer.createTopics(chn, false, Function.prototype)
      }
    };

    /**
     * Uses the consumer to promise to subscribe to a topic.
     *
     * @param {string} topic to subscribe to
     * @param {Kafka~subscribeCallback}
     * @api private
     */
    subscribe (channel, callback) {
      let p = this.options.partition || 0
      let chn = this.safeTopicName(channel)

      debug('subscribing to %s', chn)
      //debug('this.consumer', this.consumer)
      return this.consumer.addTopicsAsync([{ topic: chn, partition: p }])
        .then(()=> {
          if (callback) callback(null)
        })
        .catch(err => {
          this.onError(err)
          if (callback) callback(err)
        })
    }

    /**
     * Uses the producer to promise to send a message to kafka. Uses snappy compression.
     *
     * @param {string} topic to publish on
     * @param {object} packet to emit
     * @param {object} options
     * @api private
     */
    publish (channel, packet, opts) {
      let msg = JSON.stringify([this.uid, packet, opts])
      let chn = this.safeTopicName(channel)

      return this.producer.sendAsync([{ topic: chn, messages: [msg], attributes: 2 }])
        .then(data => {
          debug('new offset in partition:', data)
        })
        .catch(err => this.onError(err))
    };

    /**
     * Broadcasts a packet.
     *
     * If remote is true, it will broadcast the packet. Else, it will also
     * produces a new message in one of the kafka topics (channel or rooms).
     *
     * @param {object} packet to emit
     * @param {object} options
     * @param {Boolean} whether the packet came from another node
     * @api public
     */
    broadcast (packet, opts, remote) {
      debug('broadcasting packet', packet, opts)
      Adapter.prototype.broadcast.call(this, packet, opts)

      if (!remote) {
        let channel = this.safeTopicName(this.mainTopic)
        this.publish(channel, packet, opts)
      }
    }

  }


  return Kafka

}

module.exports = adapter
