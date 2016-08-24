package com.kunyandata.backtesting.io

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
class KafkaProducerHandler(brokerList: String, topic: String) {

  private var producer: Producer[String, String] = null

  def sendMessage(message: String): Unit = {
    val keyedMessage = new KeyedMessage[String, String](topic, message)
    println(System.currentTimeMillis())
    producer.send(keyedMessage)
    println(System.currentTimeMillis())
  }

  def close(): Unit = producer.close()

}

object KafkaProducerHandler {

  def apply(brokerList: String, topic: String): KafkaProducerHandler = {

    val handler = new KafkaProducerHandler(brokerList, topic)

    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")

    val config = new ProducerConfig(props)
    handler.producer = new Producer[String, String](config)

    sys.addShutdownHook {
      handler.producer.close()
    }

    handler
  }
}