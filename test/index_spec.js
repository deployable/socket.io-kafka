/* globals expect: true, sinon: true, it: true, beforeEach: true */

const kafkaAdapter = require('../index.js')
const kafka = require('kafka-node')



describe('module:socket.io-kafka', function () {

  let opts, Adapter, instance

  beforeEach(function () {
    opts = {}
  })


  describe('adapter', function () {
    
    it('should return a Kafka function', function () {
      let adapter = kafkaAdapter('localhost:2821')
      expect(adapter.name).to.equal('Kafka')
      expect(adapter).to.be.a.function
    })

    it('should create a kafka.Producer instance if producer not provided', function(){
      kafkaAdapter('localhost:2821', opts)
      expect(opts.producer).to.be.an.instanceof(kafka.Producer)
    })

    it('should create a kafka.Consumer instance if consumer not provided', function(){
      kafkaAdapter('localhost:2821', opts)
      expect(opts.consumer).to.be.an.instanceof(kafka.Consumer)
    })


    describe('only options object', function () {

      it('should accept only the options object', function () {
        opts.host = 'localhost'
        opts.port = 2821
        kafkaAdapter(opts)
        expect(opts.producer).to.be.ok
      })

      it('should accept only the options object with uri', function () {
        opts.uri = 'localhost:2821'
        kafkaAdapter(opts)
        expect(opts.producer).to.be.ok
      })

      it('should throw if uri or host/port are defined', function () {
        expect( ()=> kafkaAdapter(opts) ).to.throw()
      })

    })

  })


  describe('adapter.Kafka', function () {

    describe('without producer', function () {

      beforeEach(function () {
        opts.producer = { on: sinon.spy() }
        Adapter = kafkaAdapter('localhost:2821', opts)
        instance = new Adapter({name: '/'})
      })

      it('should have a uid', function () {
        expect(typeof instance.uid).to.equal('string')
        expect(instance.uid).to.be.a('string')
      })

      it('should have a kafka.Consumer', function () {
        expect(instance.consumer).to.be.an.instanceof(kafka.Consumer)
      })

      it('should have a kafka.Producer', function () {
        delete opts.producer
        Adapter = kafkaAdapter('localhost:2821', opts)
        instance = new Adapter({name: '/'})
        expect(instance.producer).to.be.an.instanceof(kafka.Producer)
      })

      it('should default mainTopic to socket.io/', function () {
        expect(instance.mainTopic).to.equal('socket.io-kafka-group/')
      })

      it('should default createTopics to true', function () {
        expect(instance.options.createTopics).to.equal(true)
      })

      it('should expect the ready event on consumer', function () {
        expect(opts.producer.on.calledWith('ready')).to.be.ok
      })

    })


    describe('producer ready', function () {

      beforeEach(function () {
        opts.consumer = { 
          on: sinon.spy(), 
          addTopicsAsync: () => new Promise(resolve => resolve(true))
        }
        Adapter = kafkaAdapter('localhost:2821', opts)
        instance = new Adapter({name: '/'})
        sinon.spy(instance, 'createTopic')
        sinon.spy(instance, 'subscribe')
        opts.producer.emit('ready')
      })

      it('should call createTopic', function () {
        expect(instance.createTopic.calledWith(instance.mainTopic)).to.be.ok
      })

      it('should call subscribe', function () {
        expect(instance.subscribe.calledWith(instance.mainTopic)).to.be.ok
      })
    })
  })


  describe('adapter.Kafka#onError', function () {

    beforeEach(function () {
      //opts.producer = { on: sinon.spy() }
      Adapter = kafkaAdapter('localhost:2821', opts)
      instance = new Adapter({name: '/'})
      sinon.spy(instance, 'emit')
    })

    it('should not emit error if param is falsy', function () {
      instance.onError()
      expect(instance.emit.called).to.be.false
    })

    it('should emit error if param is truthy', function () {
      expect( () => instance.onError(new Error('my error')) ).to.throw('my error')
    })
  })

  describe('adapter.Kafka#onMessage', function () {
    
    let msg, kafkaMsg
    
    beforeEach(function () {
      msg = ['abc', {message: 'test'}, {}]
      kafkaMsg = { value: JSON.stringify(msg) }

      Adapter = kafkaAdapter('localhost:2821', opts)
      instance = new Adapter({name: '/'})
      sinon.spy(instance, 'broadcast')
      sinon.spy(instance, 'onError')
    })

    it('should add a default nsp if none in packet', function () {
      msg[1].nsp = '/'
      instance.onMessage(kafkaMsg)
      expect( instance.broadcast.firstCall.args ).to.eql([ 
        msg[1], msg[2], true
      ])
    })

    it('should ignore a different name space', function () {
      msg[1].nsp = '/ASDFASKLFSDAKFLSADFDSAKAL'
      kafkaMsg = { value: JSON.stringify(msg) }
      instance.onMessage(kafkaMsg)
      expect( instance.broadcast.called ).to.be.false
    })

    it('should ignore a message without packet', function () {
      msg = ['abc']
      kafkaMsg = { value: JSON.stringify(msg) }
      instance.onMessage(kafkaMsg)
      expect( instance.broadcast.called ).to.be.false
    })

    it('should emit an error if recieving no message', function () {
      let fn = () => instance.onMessage()
      expect( fn, 'onError called' ).to.throw('No message')
    })

    it('should emit an error if recieving a bad message', function () {
      let fn = () => instance.onMessage({})
      expect( fn, 'onError called' ).to.throw('No message.value')
    })


    it('should emit an error if param is not a valid JSON', function () {
      kafkaMsg.value = '{abcd}'
      let fn = () => instance.onMessage(kafkaMsg)
      expect( fn, 'onError called' ).to.throw(SyntaxError, /Unexpected token/)
    })

  })


  describe('adapter.Kafka#safeTopicName', function () {

    beforeEach(function () {
      Adapter = kafkaAdapter('localhost:2821', opts)
      instance = new Adapter({name: '/'})
    })

    it('should remove the slashes from the channel name', function () {
      let name = instance.safeTopicName('socket.io/test')
      expect(name).to.equal('socket.io_test')
    })

  })


  describe('adapter.Kafka#createTopic', function () {

    beforeEach(function () {
      opts.producer = {
        on: sinon.spy(),
        createTopics: sinon.spy()
      }

      Adapter = kafkaAdapter('localhost:2821', opts)
      instance = new Adapter({name: '/'})
    })

    it('should create a new topic in kafka', function () {
      instance.createTopic('socket.io/test')
      expect(opts.producer.createTopics.calledWith('socket.io_test')).to.be.ok
    })

    it('should not create topic if createTopics is false', function () {
      opts.createTopics = false
      instance.createTopic('socket.io/test')
      expect(opts.producer.createTopics.called).to.be.false
    })
  })


  describe('adapter.Kafka#subscribe', function () {

    let spy

    beforeEach(function () {
      spy = sinon.spy()
      opts.consumer = {
        addTopicsAsync: (...args) => new Promise(resolve => {
          spy(...args)
          resolve(true)
        })
      }

      Adapter = kafkaAdapter('localhost:2821', opts)
      instance = new Adapter({name: '/'})
    })

    it('should add a new topic to the consumer', function () {
      instance.subscribe('socket.io/test')
      let calls = {
        topic: 'socket.io_test',
        partition: 0
      }
      expect( spy.firstCall.args[0][0], 'addTopicsAsync call' ).to.eql( calls )
    })

    it('should add a new topic to the consumer with cb', function () {
      let cbspy = sinon.spy()
      instance.subscribe('socket.io/test2', cbspy).then(()=> {
        expect( cbspy.called, 'subsscribe callback called' ).to.be.true
      })
    })

  })


  describe('adapter.Kafka#publish', function () {

    let spy

    beforeEach(function () {
      spy = sinon.spy()
      opts.producer = {
        on: sinon.spy(),
        sendAsync: (...args) => new Promise(resolve => {
          spy(...args)
          resolve(true)
        })
      }
      Adapter = kafkaAdapter('localhost:2821', opts)
      instance = new Adapter({name: '/'})
    })

    it('should send a message to kafka', function () {
      let packet = {message: 'test'}
      let msg = JSON.stringify([instance.uid, packet, {}])

      instance.publish('socket.io/test', packet, {})
      let expected_args = [{
        topic: 'socket.io_test',
        messages: [msg],
        attributes: 2
      }]
      expect( spy.firstCall.args[0], 'sendAsync call' ).to.eql( expected_args )
    })
  })


  describe('adapter.Kafka#broadcast', function () {

    let spy

    beforeEach(function () {
      spy = sinon.spy()
      opts.producer = {
        on: sinon.spy(),
        sendAsync: (...args) => new Promise(resolve => {
          spy(...args)
          resolve(true)
        })
      }
      Adapter = kafkaAdapter('localhost:2821', opts)
      instance = new Adapter({name: '/'})
    })

    it('should add nsp key to the packet', function () {
      let packet = { message: 'test' }

      instance.broadcast(packet, {}, true)
      expect( packet.nsp ).to.equal('/')
    })

    it('should send a message when remote is falsy', function () {
      let packet = {message: 'test', nsp: '/'}
      let msg = JSON.stringify([instance.uid, packet, {}])

      let expected_args = [{
        topic: 'socket.io-kafka-group_',
        messages: [msg],
        attributes: 2
      }]
      instance.broadcast(packet, {})
      expect( spy.firstCall.args[0] ).to.eql( expected_args )

    })

    it('should not send a message when remote is true', function () {
      let packet = {message: 'test'}

      instance.broadcast(packet, {}, true)
      expect( spy.called, 'sendAsync called' ).to.be.false
    })
  })

})
