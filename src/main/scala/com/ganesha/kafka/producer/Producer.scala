package com.ganesha.kafka.consumer

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Try

class Producer(bootstrapServers:String, batchSize:Integer, keySerializer: String, valueSerializer: String) {
  private val producerProperties = new Properties()
  producerProperties.put("acks", "all")
  producerProperties.setProperty("bootstrap.servers", bootstrapServers)
  producerProperties.put("batch.size", batchSize)
  producerProperties.setProperty("key.serializer", keySerializer)
  producerProperties.setProperty("value.serializer", valueSerializer)
  producerProperties
  private val kafkaProducer: KafkaProducer[Any, Any] = new KafkaProducer[Any, Any](producerProperties)

  def writeMessages(producerRecords: List[ProducerRecord[Any, Any]]): Try[Unit] = {
    Try {
      for (producerRecord <- producerRecords) {
        writeMessage(producerRecord)
      }
    }
  }

  def writeMessage(producerRecord: ProducerRecord[Any, Any]): Try[Unit] = {
    Try {
        kafkaProducer.send(producerRecord)
    }
  }
}