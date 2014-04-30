"use strict"
var redis = require("redis");
var crc16 = require('./crc16');
var commands = require('./commands');

var util = require('util');
var extend = require('extend');
var EventEmitter = require('events').EventEmitter;

var defaults = {
    hashSlots:16384,
    requestTTL : 16,
    host:'localhost',
    port:'6379',
    redis : {
      connect_timeout:1
    },
    max_connections : 10
  };

var notValidCommands = ["info","multi","exec","slaveof","config","shutdown","select"];

/**
  Creates a Redis Cluster Instance
**/


function RedisCluster(startup_nodes,opts,callback) {
  
  if (!(this instanceof RedisCluster)) {
    return new RedisCluster(startup_nodes,opts, callback);
  }
  EventEmitter.call(this);
  
  //accept null cb
  callback = callback || function nif(){};

  //accept no options parameters calling
  if(typeof opts == "function")
    callback = opts;


  var that = this;
  this.opts = extend(true, {}, defaults, opts);
  this.startup_nodes = extend(true,[],startup_nodes || [{host:this.opts.host,port:this.opts.port}])
  
  this.slots = []; //slot -> addr key value ()
  this.connections={}; //where we store our open connections
  this.sub_conn = null //special conn for subscription purposes

  this.max_connections = this.opts.max_connections; //max allowed conns to cluster
  this.cluster_ready = false;
  this.queue = [];
  
  
  this.on('error',function(err){
    /*
    var isClusterDown = /CLUSTERDOWN/.exec(err);
    if(isClusterDown){
      this.disconnect(); 
    }*/  //throw err; //cluster is down, cant continue!
    console.log(err.stack);  
    callback(err,null);  
    
  });
  
  // when we are aware of the cluster and slots sharding
  this.once('ready',function (nodes) {
    console.log('ready');
    that.startup_nodes = nodes;
    callback(null,that);
    that.cluster_ready = true;
    that.flush_queue(); // execute commands received before cluster being connected
  });

  //get nodes info and slots mapping!
  this.initialize_slots_cache(function(nodes){
    that.emit('ready',nodes);
  });
}

util.inherits(RedisCluster,EventEmitter);

/**
Get a new redis instance client
**/
RedisCluster.prototype.get_redis_link = function(host,port) {
  var that = this;
  
  var c =redis.createClient(port,host, this.opts.redis);
  
  var events=["error","message","pmessage","subscribe","psubscribe","unsubscribe","punsubscribe"];
  
  //bridiging events from each client to RedisCluster
  events.forEach(function(e){
    c.on(e,function(){
      var args = Array.prototype.slice.call(arguments);
      args.unshift(e);
      that.emit.apply(that,args);  
    })
  });
  return c;
}

/**
  Define Key for Node Identifier
**/
RedisCluster.prototype.set_node_name = function(node) {
  if(!node.name)
    node.name = node.host+':'+node.port
  return node;
};

/**
  Ask the supplied nodes for cluster information, to consolidate the actual
  hash slot, and connect to each one of them 
**/

RedisCluster.prototype.initialize_slots_cache = function(cb) {
  var that = this;
  var nodes = [];
  
  function getClusterNodes(node,callback){
    node.addr = node.host+':'+node.port;
    var r = that.get_redis_link(node.host,node.port);
    
    r.cluster('nodes', function(err, l) {
      //either way close the client stream to release fd
      r.end();
      if (err) {
        callback(err,null);
        return;
      };
      
      var lines = l.split('\n');
      (lines[lines.length-1]=='')?lines.pop():null; // split adds a empty array
      
      //nodes list
      for (var i = lines.length - 1; i >= 0; i--) {
        var items = lines[i].split(' ');
        
        //node
        var addr=(items[1] == ":0")?node.addr:items[1]; // detect current node
        var addr_port = addr.split(':');
        addr = {host:addr_port[0],port:addr_port[1],name:addr};
        nodes.push(addr);

        //populating slots
        if(items[8]){
          var pslots = items[8].split('-');
          for (var j=parseInt(pslots[0]),max=parseInt(pslots[1]);j<= max;j++) {
            that.slots[j]=addr;
          };
        }
      };
      callback(null,nodes);
    });
  }
  
  function getClusterNodesCb(err,nodes){
    if(err){
      that.emit('error',new Error(err));
      console.log('error on getNodes',that.startup_nodes)
      iterateNodes(that.startup_nodes.shift()); //get next one!
    }else{
      if(typeof cb == "function")
        cb(nodes);
    }
  }

  function iterateNodes(n){
    console.log('N',n);
    if(n)
      getClusterNodes(n,getClusterNodesCb);
    else
      that.emit('error',new Error('Cant reach any node of the cluster...CLUSTERDOWN?'));
  };
  
  iterateNodes(that.startup_nodes.shift());
};

/**
  Return the hash slot from the key
**/

RedisCluster.prototype.keyslot = function(key) {
    var s,e;
    if(s = key.indexOf('{')!=-1){
      if(e.indexOf(('}')!=-1))
        key = key.substring(s+1,e);
    }
    return crc16(key) % this.opts.hashSlots;
};

RedisCluster.prototype.set_connection = function(node,conn) {
  node = this.set_node_name(node);
  if(!this.connections[node.name]){
    this.close_existing_connection();
    this.connections[node.name] = conn || this.get_redis_link(node.host,node.port);
  }
  return this.connections[node.name];
};


/**
  If the current number of connections is already the maximum number
  allowed, close a random connection. This should be called every time
  we cache a new connection in the @connections hash.
**/
RedisCluster.prototype.close_existing_connection = function() {
  console.log("conn",Object.keys(this.connections).length,"max_connections",this.max_connections);
  if(Object.keys(this.connections).length >= this.max_connections){
    var c = Object.keys(this.connections)[0];
    if (this.connections.hasOwnProperty(c)) {
        console.log('deleting',this.connections[c].port);
        this.connections[c].end(); // disconnect
        delete this.connections[c]; //delete from connections
        this.close_existing_connection();
    }
    else
      this.emit("error",new Error("Connections Object got corrupted!"));
  }
};

/**
  Return a link to a random node, or raise an error if no node can be
  contacted. This function is only called when we can't reach the node
  associated with a given hash slot, or when we don't know the right
  mapping.
  
  The function will try to get a successful reply to the PING command,
  otherwise the next node is tried.
**/
RedisCluster.prototype.get_random_connection = function(cb) {
  var that = this;
  var nodes = this.startup_nodes.slice(); //copy startup nodes!
  
  function takeRandomNode() {
    return nodes.splice(Math.floor(Math.random() * nodes.length),1)[0];
  }
  
  function iterateNodes(n){
    if(n){
      var c = that.connections[n.name] || that.get_redis_link(n.host,n.port);
      c.ping(function(err,res){
        if(err || res !== "PONG"){
          that.emit('error',new Error(err));
          c.end(); //close the stream
          return iterateNodes(takeRandomNode()); //get random one!
        }else{
          cb(null,n,c);
        }
      });
    } else {
      var err = new Error('Cant reach any node of the cluster...CLUSTERDOWN?');
      that.emit('error',err);
      cb(err);
    }
  };
  iterateNodes(takeRandomNode());
};

/**
  Given a slot return the link (Redis instance) to the mapped node.
  Make sure to create a connection with the node if we don't have one.
**/
RedisCluster.prototype.get_connection_by_slot = function(slot,cb) {
  var that = this;
  var node = that.slots[slot];
  if(!node){
    that.get_random_connection(function(err,node,c){
      if(!err){
        var conn = that.set_connection(node,c);  
        cb(null,conn);  
      }
    });
  }else{
    var conn = that.set_connection(node);
    cb(null,conn);
  }
};
/**
  Flushes queue of commands received before cluster being ready, keeping the order.
**/
RedisCluster.prototype.flush_queue = function() {
  while(this.queue.length!=0)
    this.queue.shift()();
};

/**
  Dispatch commands
**/

RedisCluster.prototype.send_cluster_command = function(command,args,callback) {
  
  //execute command if cluster is ready, or store in queue
  if(!this.cluster_ready){
    this.queue.push(this.send_cluster_command.bind(this,command,args,callback))
    return;
  }
  
  //check if the callback is inside the args
  var last_arg_type = typeof args[args.length - 1];
  if (callback === undefined && last_arg_type === "function" || last_arg_type === "undefined") {
    callback = args.pop();
  };

  //check if its subscription/unsubscription command, since this ones keep a open stream, that does not need key/mapping
  if (command === "subscribe" || command === "psubscribe" || command === "unsubscribe" || command === "punsubscribe") {
    this.sub_command(command,args,callback);
  }else{ 
    this.command(command,args,callback);
  }
};


RedisCluster.prototype.sub_command = function(command,args,callback) {
  var that= this;
  //we are dealing with subscriptions commands so no need to map keys
  if(this.sub_conn===null)
    this.get_random_connection(function(err,_node,c){
      if(!err){
        that.sub_conn=c;
        that.sub_conn.send_command(command, args,callback);  
      }
    })
  else
    this.sub_conn.send_command(command, args,callback);
};

RedisCluster.prototype.command = function(command,args,callback,ttl) {
  var that= this;
  //check if we are using the right connection
  function keymoved(err) {
    if(err) {
      var match = /^(MOVED|ASK) (\d+) (.+)/.exec(err.message);
      if(match) {
        return match.slice(1,4);
      }
    }
  };
  
  // where we handle redirections of keys
  function wrap_moved_callback(err,res){
    if(err){
      var moved = keymoved(err);
      if(moved[0] == "MOVED" || moved[0] == "ASK" ){ 
        //Slot cache changed, update!
        setImmediate(that.initialize_slots_cache.bind(that));
        var addr_port = moved[2].split(':');
        that.slots[moved[1]] = {host:addr_port[0],port:addr_port[1],name:moved[2]};
        that.command(command,args,callback,--ttl);
      } else{
        //another error, humm, try with another node?!
        that.emit('error',new Error(err));
      }
    }else{
      if(typeof callback =="function")
        callback.call(that,err,res); //call callback
    }
  }

  var ttl = ttl || this.opts.requestTTL; //max number of redirections
  if(ttl>0){
    
    //get Key from command, key is always first parameter?!
    var key = args[0];
    var slot = this.keyslot(key);
    //get client && execute command
    this.get_connection_by_slot(slot,function(err,client){
      if(err){ 
        callback(err,null);
      }else{
        if(client.pub_sub_mode)
          console.log('client',client);            
        client.send_command(command, args, wrap_moved_callback);
      }
    });
  }else{
    this.emit('error',new Error('Too many Cluster redirections?'));
  }

};


/**
  Flush the nodes info, mostly useful for debugging
**/
RedisCluster.prototype.clear_nodes = function() {
  this.startup_nodes = [];
};
/**
  Flush the cache, mostly useful for debugging when we want to force
  redirection.
**/
RedisCluster.prototype.flush_slots_cache = function() {
  this.slots = [];
};
/**
  Closes all connections and clears all mappings, good for testing purposes
**/
RedisCluster.prototype.disconnect = function() {
  var that =this;
  //close all command connections
  for(var c in this.connections){
    if (this.connections.hasOwnProperty(c)) {
      this.connections[c].end(); // disconnect
      delete this.connections[c]; //delete from connections
    }
  };
  
  //close subscription connection
  if(this.sub_conn){
    this.sub_conn.end(); 
    this.sub_conn = null;
  }

  //clear all events
  Object.keys(this._events).forEach(function(e){
    if(that._events.hasOwnProperty(e))
    that.removeAllListeners(e);
  });
  
  this.flush_slots_cache();
  this.clear_nodes();
  this.cluster_ready = false;
};


/** 
CLUSTER PUBLIC API : Methods supported by redis in cluster mode

  supports  rc.set(["foo","bar"]);
            rc.set(["foo","bar"],function(err,res){});

            rc.set("foo","bar");
            rc.set("foo","bar",function(err,res){});

**/
commands.forEach(function (fullCommand) {
    var command = fullCommand.split(' ')[0];
    //not a valid cluster command!
    if(notValidCommands.indexOf(command)==-1){
      //set up the method
      RedisCluster.prototype[command] = function (args, callback) {

          if (Array.isArray(args) && ((typeof callback === "function") || (typeof callback === "undefined"))){
              return this.send_cluster_command(command, args, callback);
          } else {
            return this.send_cluster_command(command, Array.prototype.slice.call(arguments));
          };
      };
      RedisCluster.prototype[command.toUpperCase()] = RedisCluster.prototype[command];
    }
});

// Stash auth for connect and reconnect.  Send immediately if already connected.
RedisCluster.prototype.auth = function () {
    //#todo support auth of each node of the cluster
    return null;
};
RedisCluster.prototype.AUTH = RedisClient.prototype.auth;


module.exports.Cluster = RedisCluster;
