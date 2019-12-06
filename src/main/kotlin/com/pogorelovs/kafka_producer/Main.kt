package com.pogorelovs.kafka_producer

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
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