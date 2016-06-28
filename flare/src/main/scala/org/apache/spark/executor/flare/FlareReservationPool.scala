package org.apache.spark.executor.flare


import scala.collection.mutable.{HashMap, HashSet}
import org.apache.spark.Logging
import org.apache.spark.flare.FlareCluster
import org.apache.spark.scheduler.flare._


private[spark] sealed trait FlareReservationPool {
  def name: String

  def maxShare: Option[Int]
  def minShare: Option[Int]
  def weight: Option[Int]

  def runningTasks: Int
  
  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlareReservationGroupDescription]): Unit
  
  def removeReservation(reservationId: FlareReservationId): Unit
    
  def addRunningTask(reservationId: FlareReservationId): Unit
  
  def removeRunningTask(reservationId: FlareReservationId): Unit
  
  def nextReservation: Option[FlareReservationId]
}

private[spark] class FlareReservationPoolGroup(
    val name: String,
    val minShare: Option[Int],
    val maxShare: Option[Int],
    val weight: Option[Int])(
    implicit val cluster: FlareCluster)
  extends FlareReservationPool with Logging {
  val children = new HashSet[FlareReservationPool]
  val reservationToChild = new HashMap[FlareReservationId, FlareReservationPool]
  val groupToChild = new HashMap[String, FlareReservationPool]

  val runningTaskCounter = cluster.counter(s"PoolRunningTasks:$name")

  def runningTasks = runningTaskCounter.get().toInt

  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlareReservationGroupDescription]) = {
    if (groups.isEmpty) {
      val pool = new FlareStageReservationPool(reservationId, count, this)
      reservationToChild(reservationId) = pool
      children.add(pool)
    } else {
      val childGroup = groups.head
      val pool = groupToChild.getOrElseUpdate(childGroup.name, {
        val group = new FlareReservationPoolGroup(s"$name.${childGroup.name}", childGroup.minShare, childGroup.maxShare, childGroup.weight)
        children.add(group)
        group
      })
      reservationToChild(reservationId) = pool
      pool.addReservation(reservationId, count, groups.tail)
    }
  }

  def removeReservation(reservationId: FlareReservationId) = {
    reservationToChild(reservationId) match {
      case poolGroup: FlareReservationPoolGroup => {
        reservationToChild -= reservationId
        poolGroup.removeReservation(reservationId)
      }
      case stagePool: FlareStageReservationPool => {
        reservationToChild -= reservationId
      }
    }
  }

  def addRunningTask(reservationId: FlareReservationId) = {
    runningTaskCounter.increment()
    reservationToChild.get(reservationId) match {
      case Some(pool) => pool.addRunningTask(reservationId)
      case None => logError(s"Could not find child pool for $reservationId")
    }
  }

  def removeRunningTask(reservationId: FlareReservationId) = {
    runningTaskCounter.decrement()
    reservationToChild.get(reservationId) match {
      case Some(pool) => pool.removeRunningTask(reservationId)
      case None => logError(s"Could not find child pool for $reservationId")
    }
  }

  def nextReservation: Option[FlareReservationId] = {
    if (runningTasks < maxShare.getOrElse(Integer.MAX_VALUE)) {
      //map with runningTasks to lock view of counters during sorting
      for ((child, runningTasks) <- children.map(pool => (pool, pool.runningTasks)).toSeq.sortWith(FlareReservationPool.FairComparator)) {
        val next = child.nextReservation
        if (next.isDefined)
          return next
      }
    }

    return None
  }
}

private[spark] class FlareStageReservationPool(
    reservationId: FlareReservationId,
    var count: Int,
    parent: FlareReservationPoolGroup)
  extends FlareReservationPool{

  def maxShare: Option[Int] = None
  def minShare: Option[Int] = None
  def weight: Option[Int] = None

  lazy val name = s"${parent.name}[${reservationId.driverId}][${reservationId.stageId}][${reservationId.attemptId}]"

  var runningTasks = 0

  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlareReservationGroupDescription]) = {
    this.count += count
  }
  
  def removeReservation(reservationId: FlareReservationId) = {
    this.count = 0
  }
  
  def addRunningTask(reservationId: FlareReservationId) = {
    runningTasks += 1
  }
  
  def removeRunningTask(reservationId: FlareReservationId) = {
    runningTasks -= 1
  }
  
  def nextReservation: Option[FlareReservationId] = {
    if (count > 0) {
      count -= 1
      Some(reservationId)
    } else None
  }
}

private[spark] object FlareReservationPool extends Logging{
  def createRootPool(implicit cluster: FlareCluster): FlareReservationPool = new FlareReservationPoolGroup("root", None, None, None)

  def FairComparator(
    pool1: (FlareReservationPool, Int),
    pool2: (FlareReservationPool, Int)): Boolean = {
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

