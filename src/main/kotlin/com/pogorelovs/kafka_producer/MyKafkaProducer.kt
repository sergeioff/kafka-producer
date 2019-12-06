package com.pogorelovs.kafka_producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.system.exitProcess

fun main(args: Array<String>) {

    if (args.size < 3) {
        println("Usage: $0 broker topic file")
        println("Example: $0 localhost:9092 test input")
        exitProcess(1)
    }

    val broker = args[0]
    val topic = args[1]
    val file = args[2]

    val producer = MyKafkaProducer(broker, topic)

    Files.lines(Path.of(file)).forEach {
        producer.produce(it)
    }
}

class MyKafkaProducer(brokers: String, private val topic: String) {
    private val producer = createProducer(brokers)

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        return KafkaProducer<String, String>(props)
    }

    fun produce(msg: String) {
        producer.send(ProducerRecord(topic, msg))
    }
}