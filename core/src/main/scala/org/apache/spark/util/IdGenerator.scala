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

import org.apache.spark.{SparkConf, SparkContext}

private[spark] sealed trait IdGenerator[T] {
  def next(): T
}

private object IdGenerator {
  def getData(conf: SparkConf) = conf.getOption("encodedIdData").map(_.toInt)
  def getData(sc: SparkContext) = sc.getEncodedIdData
}

private[spark] class LongIdGenerator(data: => Option[Int] = None) extends IdGenerator[Long] {
  val value = new AtomicLong()
  override def next(): Long = {
    def nextValue = value.getAndIncrement()
    data.fold(nextValue)(EncodedId(nextValue, _))
  }
}

private[spark] object LongIdGenerator {
  def apply(): LongIdGenerator = new LongIdGenerator()

  def apply(conf: SparkConf): LongIdGenerator = new LongIdGenerator(IdGenerator.getData(conf))
  def apply(sc: SparkContext): LongIdGenerator = new LongIdGenerator(IdGenerator.getData(sc))
}

private[spark] class IntegerIdGenerator(data: => Option[Int] = None) extends IdGenerator[Int] {
  val value = new AtomicInteger()
  override def next(): Int = {
    def nextValue = value.getAndIncrement()
    data.fold(nextValue)(EncodedId(nextValue, _))
  }
}

private[spark] object IntegerIdGenerator {
  def apply(): IntegerIdGenerator = new IntegerIdGenerator()

  def apply(conf: SparkConf): IntegerIdGenerator = new IntegerIdGenerator(IdGenerator.getData(conf))
  def apply(sc: SparkContext): IntegerIdGenerator = new IntegerIdGenerator(IdGenerator.getData(sc))
}