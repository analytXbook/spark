package org.apache.spark.executor.flare

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import scala.collection.mutable.HashMap
import org.apache.spark.Logging
import org.apache.spark.scheduler.flare._

private[spark] sealed trait FlareReservationPool {
  def maxShare: Int
  def weight: Double
  
  def runningTasks: Int
  
  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlareReservationGroupDescription]): Unit
  
  def removeReservation(reservationId: FlareReservationId): Unit
    
  def addRunningTask(reservationId: FlareReservationId): Unit
  
  def removeRunningTask(reservationId: FlareReservationId): Unit
  
  def nextReservation: Option[FlareReservationId]
}

private[spark] class FlareReservationPoolGroup(
    name: String,
    val maxShare: Int,
    val weight: Double)
  extends FlareReservationPool with Logging {
  val children = new ConcurrentLinkedQueue[FlareReservationPool]
  val reservationToChild = new HashMap[FlareReservationId, FlareReservationPool]
  val groupToChild = new HashMap[String, FlareReservationPool]
  
  def runningTasks = children.asScala.foldLeft(0)(_ + _.runningTasks)
  
  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlareReservationGroupDescription]) = {
    if (groups.isEmpty) {
      val pool = new FlareReservationPoolNode(reservationId, count, this)
      reservationToChild(reservationId) = pool
      children.add(pool)
    } else {
      val headGroup = groups.head
      val pool = groupToChild.getOrElseUpdate(headGroup.name, {
        val newPool = new FlareReservationPoolGroup(headGroup.name, headGroup.maxShare, headGroup.weight)
        children.add(newPool)
        newPool
      })
      reservationToChild(reservationId) = pool
      pool.addReservation(reservationId, count, groups.tail)
    }
  }
  
  def removeReservation(reservationId: FlareReservationId) = {
    reservationToChild(reservationId) match {
      case groupPool: FlareReservationPoolGroup => {
        reservationToChild -= reservationId
        groupPool.removeReservation(reservationId)
      }
      case _: FlareReservationPoolNode => {
        reservationToChild -= reservationId
      }
    }
  }
    
  def addRunningTask(reservationId: FlareReservationId) = {
    reservationToChild.get(reservationId) match {
      case Some(pool) => pool.addRunningTask(reservationId)
      case None => logError(s"Could not find child pool for $reservationId")
    }
  }
  
  def removeRunningTask(reservationId: FlareReservationId) = {
    reservationToChild.get(reservationId) match {
      case Some(pool) => pool.removeRunningTask(reservationId)
      case None => logError(s"Could not find child pool for $reservationId")
    }
  }
  
  def nextReservation: Option[FlareReservationId] = {
    for (child <- children.asScala.toSeq.sortWith(FlareReservationPool.FairComparator)) {
      val next = child.nextReservation
      if (next.isDefined)
        return next
    }

    return None
  }
}

private[spark] class FlareReservationPoolNode(
    reservationId: FlareReservationId,
    var count: Int,
    parent: FlareReservationPoolGroup)
  extends FlareReservationPool{
  var _runningTasks = 0
  def runningTasks = _runningTasks
  
  def maxShare = parent.maxShare
  def weight = parent.weight
  
  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlareReservationGroupDescription]) = {
    this.count += count
  }
  
  def removeReservation(reservationId: FlareReservationId) = {
    this.count = 0
  }
  
  def addRunningTask(reservationId: FlareReservationId) = {
    _runningTasks += 1
  }
  
  def removeRunningTask(reservationId: FlareReservationId) = {
    _runningTasks -= 1
  }
  
  def nextReservation: Option[FlareReservationId] = {
    if (count > 0) {
      count -= 1
      Some(reservationId)
    } else None
  }
}

private[spark] object FlareReservationPool {
  def createRootPool: FlareReservationPool = new FlareReservationPoolGroup("root", Int.MaxValue, 1)

  def FairComparator(p1: FlareReservationPool, p2: FlareReservationPool): Boolean = {
    val p1MaxShare = p1.maxShare
    val p2MaxShare = p2.maxShare
    val p1RunningTasks = p1.runningTasks
    val p2RunningTasks = p2.runningTasks
    val p1Needy = p1RunningTasks < p1MaxShare
    val p2Needy = p2RunningTasks < p2MaxShare

    var compare: Int = 0

    if (p1Needy && !p2Needy) {
      return true
    } else if (!p1Needy && p2Needy) {
      return false
    } else if (p1Needy && p2Needy) {
      val p1MaxShareRatio = p1RunningTasks.toDouble / math.max(p1MaxShare, 1.0)
      val p2MaxShareRatio = p2RunningTasks.toDouble / math.max(p2MaxShare, 1.0)
      compare = p1MaxShareRatio.compareTo(p2MaxShareRatio)
    } else {
      val p1TaskToWeightRatio = p1RunningTasks.toDouble / p1.weight
      val p2TaskToWeightRatio = p2RunningTasks.toDouble / p2.weight
      compare = p1TaskToWeightRatio.compareTo(p2TaskToWeightRatio)
    }

    if (compare < 0) true
    else if (compare > 0) false
    else p1RunningTasks <= p2RunningTasks
  }
}

