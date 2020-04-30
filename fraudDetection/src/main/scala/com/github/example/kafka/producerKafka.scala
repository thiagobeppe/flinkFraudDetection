package com.github.example.kafka

import java.io.{BufferedReader, FileReader}
import java.util.Properties
import java.util.UUID.randomUUID

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

object producerKafka extends App{
  val log: Logger  = LoggerFactory.getLogger(getClass.getSimpleName)
  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("ack","1")

  val file: BufferedReader =  new BufferedReader(new FileReader( "../datasets/bank_data.txt"))

  val producer: KafkaProducer[String,String] = new KafkaProducer[String, String](props)
  val topic: String = "TRANSACTIONS_TOPIC"

  val lines: Stream[String] = Stream.continually(file.readLine()).takeWhile(_ != null)
  try {
    lines.foreach(line => {
      val record: ProducerRecord[String, String] =  new ProducerRecord[String, String](topic, randomUUID().toString, line)
      val metadata = producer.send(record)
      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(),
        metadata.get().partition(),
        metadata.get().offset())
      Thread.sleep(500)
    })
  }
}
