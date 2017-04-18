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

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable

trait IdGenerator[T] {
  def next(): T
}

object IdGenerator {
  private var source: IdSource = IdSource.default

  def setSource(source: IdSource) = {
    this.source = source
  }

  def int(idGroup: String) = new IdGenerator[Int] {
    override def next(): Int = source.nextInt(idGroup)
  }

  def long(idGroup: String) = new IdGenerator[Long] {
    override def next(): Long = source.nextLong(idGroup)
  }
}

trait IdSource {
  def nextInt(idGroup: String): Int
  def nextLong(idGroup: String): Long
}

object IdSource {
  def default = new IdSource {
    val longCounters = mutable.Map[String, AtomicLong]()
    val intCounters = mutable.Map[String, AtomicInteger]()

    override def nextInt(idGroup: String): Int =
      intCounters.getOrElseUpdate(idGroup, new AtomicInteger()).getAndIncrement()

    override def nextLong(idGroup: String): Long =
      longCounters.getOrElseUpdate(idGroup, new AtomicLong()).getAndIncrement()
  }
}