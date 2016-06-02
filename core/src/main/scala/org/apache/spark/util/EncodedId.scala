/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
