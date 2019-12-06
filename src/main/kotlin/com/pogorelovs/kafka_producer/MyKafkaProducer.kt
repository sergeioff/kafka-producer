package com.pogorelovs.kafka_producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.system.exitProcess

fun main(args: Array<String>) {

    if (args.size < 3) {
        println("Usage: $0 broker topic pathToInputFile")
        println("Example: $0 localhost:9092 test input")
        exitProcess(1)
    }

    val broker = args[0]
    val topic = args[1]
    var filePath = args[2]

    if (filePath.startsWith("~" + File.separator)) {
        filePath = System.getProperty("user.home") + filePath.substring(1)
    }

    val producer = MyKafkaProducer(broker, topic)

    Files.lines(Paths.get(filePath)).forEach {
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