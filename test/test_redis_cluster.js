"use strict";
var assert = require("assert");
var should = require('should');
var RedisCluster = require("../lib/cluster").Cluster;
var config = require("../config");
var async = require("async");
var fs = require("fs");

/**
  #TODO TEST CLUSTERDOWN!!! 
**/

describe('Redis cluster', function(){
  var rc = null;
  console.log(rc);
  var redisNodes = config.redisNodes;
  var fakeNodes = [
      {host:'localhost',port:8000},
      {host:'localhost',port:8001},
      {host:'localhost',port:8002}
    ];

  beforeEach(function(){
    console.log('execute before each?');
    rc = new RedisCluster(redisNodes,{redis:{detect_buffers:true}}); 
  });

  afterEach(function(){
    rc.disconnect();
    //console.log('disconnected',rc);
  })
  
  it('should be emiting a ready event',function(done){
    
    rc.on('ready',function(nodes){
      assert.ok(nodes!==[],"No startup nodes on cluster?");
      console.log('ready',nodes);
      done();
    });
  });

  /*it('should be emiting errors',function(done){
    rc.on('error',function(err){
      console.log(err.stack);
      assert.ok(err!==null);
      done();
    });
    rc.emit('error',new Error("error test"));
  });*/

  it('should be ready with nodes info and slots mapped',function(done){
    rc.on('ready',function(){
      assert.ok(rc.cluster_ready);
      assert.ok(rc.startup_nodes !==[]);
      assert.ok(rc.slots !== []);
      done();
    })
  });
  /**
  rc.set(["foo","bar"]);
  rc.set(["foo","bar"],function(err,res){});
  rc.set("foo","bar");
  rc.set("foo","bar",function(err,res){});
  **/

  it("should be accepting commands in all 4 possible calling opts",function (done) {
    var errorCb = function(err){
      clearImmediate(sucess);
      done(err);
    };
    var sucess = setImmediate.bind(null,done);

    async.parallel([
      function(callback){
          rc.set(["foo","bar"],callback);
      },
      function(callback){
          rc.set("foo","bar",callback);
      }
    ], function(err, results){
        console.log('IO',err,results);
        if(!err && results.every(function(ele){return ele=="OK"})){
          rc.once("error",errorCb);
          rc.set(["foo","bar"]);
          rc.set("foo","bar");
          sucess();
        }else{
          console.log('ERR',err);
        }
    });
  });

  it('should queue commands sent before ready event',function(done){
    rc.cluster_ready.should.be.false;
    rc.set(["foo","bar"],function(err,res){
      res.should.equal("OK");
      done();
    });
  });
  
  it('should try all "fake nodes" with initilialize slots and fail',function(){
    
    var rc1 = new RedisCluster(fakeNodes);
    rc1.on('ready',function(){
      assert.fail('Cluster should not be ready!');
    });
    //change startup_nodes to check failing
    rc1.slots.should.be.empty;
  });

  it('should try all "fake nodes" with get_random_connection and fail',function(done){
    var rc1 = new RedisCluster(fakeNodes);
    // console.log('RC',rc);
    
    rc1.on('ready',function(){
      assert.fail('Cluster should not be ready!'); 
    });

    rc1.get_random_connection(function(err,c){
      console.log('get_random_connection',err,c);
      if(err)done();
    });
  });
  
  it('should cache the slot',function(done){
    rc.on('ready',function () {
        console.log('RC1 ready',rc.startup_nodes);
        var slot = rc.keyslot('foo');
        var node = rc.slots[slot];
        node.should.have.property("name");
        rc.get_connection_by_slot(slot,function(err,c){
          rc.connections[node.name].should.eql(c);
          done();
        });
    });
  });

  it('should redirect and rebuild cache',function(done){
    var i= 0;
    rc.on('ready',function(){
      console.log('READY EVENT',++i);
      rc.flush_slots_cache();
      rc.set(["foo","bar"],function(err,res){
        assert.ok(err == null);
        assert.ok(res == "OK");
        done();
      });
    })
  });

  it('should not have more then max_connections at one time',function(done){
    rc.on('ready',function(){
      //opens a connection for each master node of the cluster
      console.log('CONN before',Object.keys(rc.connections).length);
      rc.max_connections = 1;
      for (var i = 0; i < 10; i++) {
        rc.set(i.toString(),"foo");
      }
      var conns = Object.keys(rc.connections).length;
      conns.should.be.equal(rc.max_connections);
      done();
    });
  });
  it("should be disconnected from the cluster",function(done){
    rc.disconnect();
    setImmediate(function(){
      rc.connections.should.be.empty;
      rc.slots.should.be.empty;
      assert.ok(rc.sub_conn == null);
      rc.startup_nodes.should.be.empty;
      rc.cluster_ready.should.be.false;
      done();
    });
  });

  it('should be subscribing,publishing, and unsubscribing',function(done){
    var topic1 = "/s/1/data";
    var i = 0;
    var max = 1;
    
    //each message event
    rc.on('message',function (channel, message) {
      if(message=="fail")
        assert.fail("This message should not be received, unsubscribe fail!");
      if(message==max){
        rc.unsubscribe(topic1,function(err,topic){
          console.log(message,'messages, published in ',topic,'UNSCRIBED NOW!');
          rc.publish(topic1,'fail');
          done();
        });
      }
    });  
    
    //publish messages on topic1
    rc.subscribe(topic1,function(err,topic){
      console.log("SUBSCRIBED TO TOPIC, start publishing!",topic);
      for (i; i <=max; i++) {
        rc.publish(topic1,i);
      };
    });
  });

  it('should be psubscribing,publishing, and punsubscribing',function(done){
    var allSensors = "/ss/*/data";
    var i = 0;
    var max = 1;
    
    //each message event
    rc.on('pmessage',function (p,t, m) {
      console.log('pattern',p,"topic",t,"message",m);
      if(m=="fail")
        assert.fail("This message should not be received, unsubscribe fail!");
      if(m==max)
        done();
    });  
    
    //publish messages on topic1
    rc.psubscribe(allSensors);
    rc.on('psubscribe',function(p,t,c){
      console.log('subscribed to pattern',p,"topic",t,"count",c);
      rc.publish("/ss/1/info",'fail');
      for (i; i <=max; i++) {
        var t = "/ss/"+i+"/data";
        rc.publish(t,i);
      };
    });
  });

  it("should publish a binary payload into redis-cluster", function(done) {
    
    var expected = fs.readFileSync(__dirname + "/image.jpg");
    rc.on('message',function(channel,message){
      console.log('message',channel,message);
      assert.ok(message.toString('utf8'),expected.toString('utf8')); 
      done();
    });
    
    rc.subscribe("image", function() {
      rc.publish(["image", expected]);
    });

  });

});
