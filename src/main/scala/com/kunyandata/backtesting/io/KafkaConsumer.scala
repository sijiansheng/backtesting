package com.kunyandata.backtesting.io

import java.util.Properties

import kafka.consumer.{ConsumerConnector, Consumer, ConsumerConfig}

/**
  * Created by YangShuai
  * Created on 2016/8/23.
  */
object KafkaConsumer {

  var consumerConnector: ConsumerConnector = null

  def getInstance(zookeeper: String, groupId: String, topic: String): ConsumerConnector = {

    if (consumerConnector != null)
      return consumerConnector

    val config = createConsumerConfig(zookeeper, groupId)
    consumerConnector = Consumer.create(config)

    sys.addShutdownHook {
      consumerConnector.shutdown()
    }

    consumerConnector
  }

  def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {

    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("auto.offset.reset", "largest")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    val config = new ConsumerConfig(props)

    config
  }

}
