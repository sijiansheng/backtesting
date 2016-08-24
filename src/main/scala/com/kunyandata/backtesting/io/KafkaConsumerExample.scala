package com.kunyandata.backtesting.io


import java.util.Properties
import java.util.concurrent._
import scala.collection.JavaConversions._
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.utils._
import kafka.utils.Logging
import kafka.consumer.KafkaStream

class ScalaConsumerExample(val zookeeper: String,
                           val groupId: String,
                           val topic: String) extends Logging {

  val config = createConsumerConfig(zookeeper, groupId)
  val consumer = Consumer.create(config)

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

  def run() = {

    val topicCountMap = Map(topic -> 1)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(topic).get

    for (stream <- streams) {
      println("enter for loop")
      val it = stream.iterator()
      println("after it")
      while (it.hasNext()) {
        println("enter while loop")
        val msg = new String(it.next().message())
        System.out.println(msg)
        println("end while")
      }
    }

  }

}

object ScalaConsumerExample extends App {
  val example = new ScalaConsumerExample("61.147.114.85:2181", "test", "test123")
  println("begin")
  example.run()
  println("end")
}
