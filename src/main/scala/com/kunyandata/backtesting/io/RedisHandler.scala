package com.kunyandata.backtesting.io

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
object RedisHandler {

  var jedisPool: JedisPool = null

  /**
    * 初始化JedisPool对象
    * @param ip redis服务器ip
    * @param port redis服务端口
    * @param auth 密码
    * @param db db索引
    * @return
    */
  def init(ip: String, port: Int, auth: String, db: Int) = {

    val config: JedisPoolConfig = new JedisPoolConfig
    config.setMaxWaitMillis(10000)
    config.setMaxIdle(10)
    config.setMaxTotal(1024)
    config.setTestOnBorrow(true)

    jedisPool = new JedisPool(config, ip, port, 20000, auth, db)

    sys.addShutdownHook {
      jedisPool.close()
    }

  }

  def getJedis: Jedis  =  {

    if (jedisPool == null) {
      throw new NullPointerException()
    }

    jedisPool.getResource
  }

}
