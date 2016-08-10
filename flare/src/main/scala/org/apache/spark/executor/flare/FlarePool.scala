package org.apache.spark.executor.flare


import scala.collection.mutable.{HashMap, HashSet}
import org.apache.spark.Logging
import org.apache.spark.flare.FlareCluster
import org.apache.spark.scheduler.flare._


private[spark] sealed trait FlarePool {
  def name: String
  def parentName: String

  def fullName:String = s"$parentName.$name"

  def maxShare: Option[Int]
  def minShare: Option[Int]
  def weight: Option[Int]

  def hasChildren: Boolean

  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlareReservationGroupDescription]): Unit

  def removeReservation(reservationId: FlareReservationId): Unit

  def addRunningTask(reservationId: FlareReservationId): Unit

  def removeRunningTask(reservationId: FlareReservationId): Unit

  def nextReservation: Option[FlareReservationId]
}

private[spark] class FlareRootPool(stats: FlarePoolBackend) extends FlarePoolGroup("root", "", None, None, None)(stats) {
  override def fullName: String = "root"
}

private[spark] class FlarePoolGroup(
    val name: String,
    val parentName: String,
    val minShare: Option[Int],
    val maxShare: Option[Int],
    val weight: Option[Int])(
    implicit val stats: FlarePoolBackend)
  extends FlarePool with Logging {
  val reservationPools = new HashMap[FlareReservationId, FlarePool]
  val children = new HashMap[String, FlarePool]

  def hasChildren: Boolean = !children.isEmpty

  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlareReservationGroupDescription]) = {
    reservationPools.get(reservationId) match {
      case Some(childGroup) => childGroup.addReservation(reservationId, count, groups)
      case None => {
        if (groups.isEmpty) {
          val stagePool = new FlareStagePool(reservationId, fullName, count)
          children(stagePool.name) = stagePool
          stats.watch(fullName, stagePool.name)
          reservationPools(reservationId) = stagePool
        } else {
          val childGroup = groups.head
          val pool = children.getOrElseUpdate(childGroup.name, {
            val groupPool = new FlarePoolGroup(childGroup.name, fullName, childGroup.minShare, childGroup.maxShare, childGroup.weight)
            stats.watch(fullName, childGroup.name)
            groupPool
          })

          reservationPools(reservationId) = pool
          pool.addReservation(reservationId, count, groups.tail)
        }
      }
    }
  }

  def removeReservation(reservationId: FlareReservationId) = {
    reservationPools.remove(reservationId).foreach { pool =>
      pool.removeReservation(reservationId)
      if (!pool.hasChildren) {
        children.remove(pool.name)
        stats.unwatch(fullName, pool.name)
      }
    }
  }

  def addRunningTask(reservationId: FlareReservationId) = {
    stats.taskStarted(parentName, name)
    reservationPools.get(reservationId) match {
      case Some(pool) => pool.addRunningTask(reservationId)
      case None => logError(s"Could not find child pool for $reservationId")
    }
  }

  def removeRunningTask(reservationId: FlareReservationId) = {
    stats.taskEnded(parentName, name)
    reservationPools.get(reservationId) match {
      case Some(pool) => pool.removeRunningTask(reservationId)
      case None => logError(s"Could not find child pool for $reservationId")
    }
  }

  def nextReservation: Option[FlareReservationId] = {
    val childrenRunningTasks = stats.childrenRunningTasks(fullName).withDefaultValue(0)

    for ((child, runningTasks) <- children.values.map(child => (child, childrenRunningTasks(child.name))).toSeq.sortWith(FlarePool.FairComparator)) {
      if (child.maxShare.fold(true)(runningTasks < _)) {
        val next = child.nextReservation
        if (next.isDefined)
          return next
      }
    }

    None
  }
}

private[spark] class FlareStagePool(
    reservationId: FlareReservationId,
    val parentName: String,
    var count: Int)(
    implicit val stats: FlarePoolBackend)
  extends FlarePool{

  def hasChildren = false

  def maxShare: Option[Int] = None
  def minShare: Option[Int] = None
  def weight: Option[Int] = None

  val name = s"driver${reservationId.driverId}_stage${reservationId.stageId}_attempt${reservationId.attemptId}"

  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlareReservationGroupDescription]) = {
    this.count += count
  }

  def removeReservation(reservationId: FlareReservationId) = {
    this.count = 0
  }

  def addRunningTask(reservationId: FlareReservationId) = {
    stats.taskStarted(parentName, name)
  }

  def removeRunningTask(reservationId: FlareReservationId) = {
    stats.taskEnded(parentName, name)
  }

  def nextReservation: Option[FlareReservationId] = {
    if (count > 0) {
      count -= 1
      Some(reservationId)
    } else None
  }
}

private[spark] object FlarePool extends Logging{
  def createRootPool(stats: FlarePoolBackend): FlarePool = new FlareRootPool(stats)

  def FairComparator(
    pool1: (FlarePool, Int),
    pool2: (FlarePool, Int)): Boolean = {
    val p1 = pool1._1
    val p2 = pool2._1

    val p1RunningTasks = pool1._2
    val p2RunningTasks = pool2._2

    //rank pools under max capacity higher
    val p1Maxed = p1.maxShare.fold(false)(p1RunningTasks >= _)
    val p2Maxed = p2.maxShare.fold(false)(p2RunningTasks >= _)

    if (p1Maxed && p2Maxed) {
      return p1.name < p2.name
    } else if (p1Maxed && !p2Maxed) {
      return false
    } else if (!p1Maxed && p2Maxed) {
      return true
    }

    //rank pools under min capacity higher
    var compare: Int = 0

    val p1MinShare = p1.minShare.getOrElse(0)
    val p2MinShare = p2.minShare.getOrElse(0)
    val p1Needy = p1RunningTasks < p1MinShare
    val p2Needy = p2RunningTasks < p2MinShare

    if (p1Needy && !p2Needy) {
      return true
    } else if (!p1Needy && p2Needy) {
      return false
    } else if (p1Needy && p2Needy) {
      val p1MinShareRatio = p1RunningTasks.toDouble / p1MinShare.max(1)
      val p2MinShareRatio = p2RunningTasks.toDouble / p2MinShare.max(1)
      compare = p1MinShareRatio.compareTo(p2MinShareRatio)
    } else {
      val p1TaskToWeightRatio = p1RunningTasks.toDouble / p1.weight.getOrElse(1)
      val p2TaskToWeightRatio = p2RunningTasks.toDouble / p2.weight.getOrElse(1)
      compare = p1TaskToWeightRatio.compareTo(p2TaskToWeightRatio)
    }

    if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      p1.name < p2.name
    }
  }
}

