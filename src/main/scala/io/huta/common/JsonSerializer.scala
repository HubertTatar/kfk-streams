package io.huta.common

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

class JsonSerializer[T] extends Serializer[T] {
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(SerializationFeature.INDENT_OUTPUT, true)
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  override def serialize(topic: String, data: T): Array[Byte] =
    Option(data).map { _ =>
      try mapper.writeValueAsBytes(data)
      catch {
        case e: Exception =>
          throw new SerializationException("Error serializing JSON message", e)
      }
    }.orNull
}
