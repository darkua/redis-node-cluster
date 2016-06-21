redis-node-cluster
==================

Redis Cluster Client with hash slot caching


Please make sure redis is correctly set up in cluster mode,

http://redis.io/topics/cluster-tutorial

Inside the cluster-test directory you find already the basic config for each redis cluster instance [7000...7005]


##### Usage

```

var config = require("../config");//redis connection info
var redisNodes = config.redisNodes; // 7000...7005
var RedisCluster = require("../lib/cluster").Cluster;
var rc = new RedisCluster(redisNodes,{redis:{detect_buffers:true}});
rc.set(["foo","bar"],console.log);
rc.get(["foo"],console.log);

```

##### test

```
npm install -g mocha
mocha

```