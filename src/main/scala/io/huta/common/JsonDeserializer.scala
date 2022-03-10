package io.huta.common

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import scala.reflect.ClassTag


class JsonDeserializer[T](implicit tag: ClassTag[T], ev: Null <:< T) extends Deserializer[T] {
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(SerializationFeature.INDENT_OUTPUT, true)
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  override def deserialize(topic: String, bytes: Array[Byte]): T =
    Option(bytes).map { _ =>
      try mapper.readValue(
        bytes,
        tag.runtimeClass.asInstanceOf[Class[T]]
      )
      catch {
        case e: Exception => throw new SerializationException(e)
      }
    }.orNull
}
