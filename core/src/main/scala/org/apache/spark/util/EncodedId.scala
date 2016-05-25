package org.apache.spark.util

object EncodedId {
  private val DATA_BITS = 8
  private val DATA_MASK = ((1 << DATA_BITS) - 1)

  private var data: Int = _

  private var _enabled = false
  def enabled = _enabled

  def enable(data: Int) = {
    EncodedId.data = data
    _enabled = true
  }

  def encode(value: Int): Int = (value << DATA_BITS) | data
  def encode(value: Long): Long = (value << DATA_BITS) | data

  def encodeIfEnabled(value: Int) = if (enabled) encode(value) else value
  def encodeIfEnabled(value: Long) = if (enabled) encode(value) else value

  def decode(value: Int): (Int, Int) = (value & DATA_MASK, value >> DATA_BITS)
  def decode(value: Long): (Int, Long) = ((value & DATA_MASK).toInt, value >> DATA_BITS)
}
