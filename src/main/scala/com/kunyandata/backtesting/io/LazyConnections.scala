package com.kunyandata.backtesting.io

import java.sql.{DriverManager, PreparedStatement}
import java.util
import java.util.Properties

import com.kunyandata.backtesting.config.Configuration
import kafka.producer.{ProducerConfig, KeyedMessage, Producer}
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Jedis}

/**
  * Created by yangshuai on 2016/3/9.
  */
class LazyConnections(createJedis: () => Jedis,
                      createProducer: () => Producer[String, String]) extends Serializable {

  lazy val jedis = createJedis()

  lazy val producer = createProducer()

  def jedisSelect(db: Int): Unit = {
    jedisConnectIfNot()
    jedis.select(db)
  }

  def jedisSet(key: String, value: String, overTime: Int): String = {
    jedisConnectIfNot()
    val result = jedis.set(key, value)
    jedis.expire(key, overTime)
    result
  }

  def jedisHset(key: String, field: String, value: String, expire: Int): Long = {
    jedisConnectIfNot()
    val result = jedis.hset(key, field, value)
    jedis.expire(key, expire)
    result
  }

  def jedisZadd(key: String, value: String, score: Double): Long = {
    jedisConnectIfNot()
    val result = jedis.zadd(key, score, value)
    result
  }

  def jedisDel(key: String): Unit = {
    jedisConnectIfNot()
    jedis.del(key)
  }

  def jedisHget(key: String, field: String): String = {
    jedisConnectIfNot()
    jedis.hget(key, field)
  }

  def jedisExpire(key: String, seconds: Int): Long = {
    jedisConnectIfNot()
    jedis.expire(key, seconds)
  }

  def jedisExists(key: String): Boolean = {
    jedisConnectIfNot()
    jedis.exists(key)
  }

  def jedisConnectIfNot(): Unit = {
    if (!jedis.isConnected) {
      jedis.connect()
    }
  }

  def jedisGetKeysLike(keys: String): util.Set[String] = {
    jedis.keys(keys)
  }

  /**
    * send message to topic: newsparser_contentparse
    */
  def sendContent(value: String): Unit = {
    val message = new KeyedMessage[String, String]("", value)
    producer.send(message)
  }

}

object LazyConnections {

  def apply(configFilePath: String): LazyConnections = {

    val configurations = Configuration.getConfigurations(configFilePath)

    val redisMap = configurations._1
    val kafkaMap = configurations._2

    val createJedis = () => {

      val config: JedisPoolConfig = new JedisPoolConfig
      config.setMaxWaitMillis(10000)
      config.setMaxIdle(10)
      config.setMaxTotal(1024)
      config.setTestOnBorrow(true)

      val jedisPool = new JedisPool(
        config,
        redisMap.get("ip").get,
        redisMap.get("port").get.toInt,
        20000,
        redisMap.get("auth").get,
        redisMap.get("db").get.toInt)

      sys.addShutdownHook {
        jedisPool.close()
      }

      jedisPool.getResource
    }

    val createProducer = () => {

      val props = new Properties()
      props.put("metadata.broker.list", kafkaMap.get("brokerList").get)
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("producer.type", "async")

      val config = new ProducerConfig(props)
      val producer = new Producer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }

    new LazyConnections(createJedis, createProducer)
  }

}

