package com.kunyandata.backtesting.io

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
object KafkaProducer {

  var producer: Producer[String, String] = null
  var topic: String = ""

  /**
    * 初始化producer
    * @param brokerList 格式："ip:port"
    * @param topic 发送结果用的topic
    */
  def initProducer(brokerList: String, topic: String): Unit = {

    if (producer != null)
      return

    this.topic = topic

    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")

    val config = new ProducerConfig(props)
    this.producer = new Producer[String, String](config)

    sys.addShutdownHook {
      producer.close()
    }

  }

  def sendContent(value: String): Unit = {
    val message = new KeyedMessage[String, String](topic, value)
    producer.send(message)
  }

}
