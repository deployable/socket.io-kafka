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
const debugmsg = require('debug')('socket.io-kafka:index:message')
const Promise = require('bluebird')
const kafka = require('kafka-node')
const Adapter = require('socket.io-adapter')
const uuid = require('uuid')

// Track clients so they can be cleaned up on exit
const kafka_adapters = {}

process.on('SIGINT',()=> cleanUpClients())
process.on('SIGTERM',()=> cleanUpClients())


// Loop though all stored client uids and close them
function cleanUpClients(){
  console.log('socketio-kafka cleaning up kafka clients')
  return Promise.map( Object.keys(kafka_adapters), uid => kafka_adapters[uid].closeClient() )
}



/**
 * Generator for the kafka Adapater
 *
 * @param {string} optional, zookeeper connection string
 * @param {object} adapter options
 * @return {Kafka} adapter
 * @api public
 */
function adapter(uri, options = {}) {
  if (!options.key) options.key = 'socketio_kafka_grp'

  // handle options only
  if (!uri ) { throw new Error('URI or options with URI require') }

  if (typeof uri === 'object') {
    options = uri
    if ( options.uri ) uri = options.uri
    else {
      if ( options.host && options.port ) `${options.host}:${options.port}`
      else throw new Error('URI or host/port are required.')
    }
  }
  clientId = options.clientId || 'socket.io-kafka'

  // Create producer and consumer if they weren't provided
  if (!options.producer || !options.consumer) {
      debug('creating new kafka client')
      
      //client = new kafka.Client(uri, clientId, { retries: 4 })
      if (!options.client) {
        options.client = new kafka.Client()
        debug('adapter created new kafka Client', options.client.zk.client.getSessionId())
      }
      
      /* istanbul ignore next */
      options.client.on('error', function (err, data) {
        console.error('error', err, data)
      })

      if (!options.producer) {
        debug('creating new kafka producer')
        options.producer = new kafka.Producer(options.client)
        //debug('created producer',options.producer)
        options.producer = Promise.promisifyAll(options.producer)
      }

      if (!options.consumer) {
        debug('creating new kafka consumer')
        options.consumer = new kafka.Consumer(options.client, [], { groupId: options.key })
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

      // Random Adapter uid
      this.uid = uuid.v4()
      this.debug = require('debug')(`socket.io-kafka:index[${this.uid}]`)
      this.options = options
      // Msg prefix
      this.prefix = options.key
      
      // Kafka-node consumer and producer instances
      this.consumer = options.consumer
      this.producer = options.producer

      // Add the socketio namespace to the kafka topic
      this.mainTopic = this.prefix + nsp.name

      // Allow this adapter to close the attached kafka client
      this.dont_close = options.dont_close

      options.createTopics = (create === undefined) ? true : create

      options.producer.on('ready', () => {
        this.debug('producer ready')
        this.createTopic(this.mainTopic)
        this.subscribe(this.mainTopic)

        // handle incoming messages to the channel
        /* istanbul ignore if */
        if (!this.consumer.on) throw new Error('Kafka consumer must have an `on` method')
        this.consumer.on('message', this.onMessage.bind(this))
        this.consumer.on('error', this.onError.bind(this))
      })

      this.storeAdapter()
    }

    storeAdapter(){
      kafka_adapters[this.uid] = this
    }

    // Accepts a client uid, and closes it
    closeClient(){
      return new Promise((resolve, reject) => {
        if ( this.dont_close || !this.options.client || !this.options.client.close ) resolve(false)
        this.options.client.close((err) => {
          if (err) return reject(err)
          console.log('closed kafka client for adapter %s', this.uid, err)
          if (kafka_adapters[this.uid]) delete kafka_adapters[this.uid]
          resolve(true)
        })
      })
    }

    /**
     * Emits the error.
     *
     * @param {object|string} error
     * @api private
     */
    onError (...errors) {
      this.debug('emitting errors', errors)
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
        debugmsg('kafkaMessage', kafkaMessage)
        if ( !kafkaMessage ) throw new Error('No message')
        if ( !kafkaMessage.value ) throw new Error('No message.value')
        message = JSON.parse(kafkaMessage.value)
        if ( this.uid === message[0] ) return this.debug('ignore same uid')
        packet = message[1]

        if ( !packet ) return this.debug('ignore bad packet')
        if ( packet.nsp === undefined ) packet.nsp = '/'
        if ( packet.nsp !== this.nsp.name ) return this.debug('ignore different namespace')

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

      this.debug('creating topic %s', chn)
      if (this.options.createTopics) {
        //this.producer.createTopics(chn, this.onError.bind(this));
        this.producer.createTopics(chn, false, Function.prototype)
      }
    }

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

      this.debug('subscribing to %s', chn)
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

      this.debug('publish to %s', channel)

      return this.producer.sendAsync([{ topic: chn, messages: [msg], attributes: 2 }])
        .then(data => {
          this.debug('new offset in partition:', data)
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
      this.debug('broadcasting packet', packet, opts)
      Adapter.prototype.broadcast.call(this, packet, opts)

      if (!remote) {
        let channel = this.safeTopicName(this.mainTopic)
        this.publish(channel, packet, opts)
      }
    }

  }


  Kafka.options = options
  return Kafka

}

module.exports = adapter
module.exports.cleanUpClients = cleanUpClients
module.exports.kafka_adapters = kafka_adapters
