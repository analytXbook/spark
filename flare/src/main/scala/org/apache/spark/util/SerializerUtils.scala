package org.apache.spark.util

import java.nio.ByteBuffer

import org.apache.spark.serializer.SerializerInstance

import scala.reflect.ClassTag

object SerializerUtils {
  implicit class PimpedSerializerInstance(serializer: SerializerInstance) {
    def toBytes[T: ClassTag](t: T): Array[Byte] = {
      val serialized = serializer.serialize(t)
      val bytes = new Array[Byte](serialized.remaining())
      serialized.get(bytes)
      bytes
    }

    def fromBytes[T: ClassTag](bytes: Array[Byte]): T = {
      serializer.deserialize(ByteBuffer.wrap(bytes))
    }
  }
}
