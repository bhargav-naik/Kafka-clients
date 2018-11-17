package com.ganesha.driver

import com.ganesha.kafka.consumer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

object ProducerApp {
  def main(args: Array[String]): Unit = {

    val bootstrapServer = args(0)
    val topicName = args(1)
    val numOfMessages = args(2).toInt

    val kafkaProducer = new Producer(bootstrapServer, 100,
      "org.apache.kafka.common.serialization.StringSerializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    for(i <- 0 to numOfMessages) {
      val k = i.toString

      val result = kafkaProducer.writeMessage(new ProducerRecord[Any, Any](topicName, k, k))
      System.out.println ("topic:" + topicName + ", key:" + k + ", val:" + k + ", result:" + result.isSuccess)
    }

  }
}
