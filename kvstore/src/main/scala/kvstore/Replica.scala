package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor }
import kvstore.Arbiter._
import akka.actor._
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import akka.actor.Stash
import scala.language.postfixOps

trait Message

object Replica {

  sealed trait Operation extends Message {
    def key: String
    def id: Long
  } 
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  case object Failure

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Stash {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var expectedSeq: Int = 0

  var persistId = 0

  var id = 0

  val applyToArbiter = arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val persistence = context.actorOf(persistenceProps)

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case insert @ Insert(key, value, _) => {
      kv += (key -> value)
      for ((s,r) <- secondaries) r ! Replicate(key, Some(value), id)
      sendPersist(insert, sender, Some(10))
    }
    case remove @ Remove(key, _) => {
      kv -= key
      for ((s,r) <- secondaries) r ! Replicate(key, None, id)
      sendPersist(remove, sender, Some(10))
    }
    case Get(key, id) => getKey(key, id, sender)
    case Replicas(replicas) => processReplicas(replicas)
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case snap @ Snapshot(key, valueOption, seq) => {
      if (seq == expectedSeq) {
        expectedSeq += 1
        valueOption match {
          case Some(value) => kv += (key -> value)
          case None => kv -= key
        }
      }
      if (seq <= expectedSeq) sendPersist(snap, sender, None)
    }
    case Get(key, id) => getKey(key, id, sender)
  }

  def persisting(message: Message, actor: ActorRef, timer: Cancellable, retries: Option[Int]): Receive = {
    case Persisted(key, _) => {
      timer.cancel()
      persistId += 1
      message match {
        case Snapshot(key, _, seq) => {
          unstashAll()
          actor ! SnapshotAck(key, seq)
          context.become(replica)
        }
        case insert @ Insert(_, _, id) => waitForAcknowledgement(insert, actor, id)
        case remove @ Remove(_, id) => waitForAcknowledgement(remove, actor, id)
      }
    }
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id) 
    case Timeout => sendPersist(message, actor, retries)
    case Replicas(replicas) => processReplicas(replicas)
    case msg => stash() 
  }

  def sendPersist(message: Message, actor: ActorRef, retries: Option[Int]) = {
    retries match {
      case Some(0) => message match {
        case Insert(_, _, id) => replyOperation(actor, OperationFailed(id))
        case Remove(_, id) => replyOperation(actor, OperationFailed(id))
        case _ =>
      }
      case _ => {
        message match {
          case Snapshot(key, valueOption, seq) => persistence ! Persist(key, valueOption, persistId)
          case Insert(key, value, _) => persistence ! Persist(key, Some(value), persistId)
          case Remove(key, _) => persistence ! Persist(key, None, persistId)
        }
        val timer = context.system.scheduler.scheduleOnce(100 millis, self, Timeout)
        context.become(persisting(message, actor, timer, retries.map(_-1)))
      }
    }
  }

  def waitForAcknowledgement(op: Operation, actor: ActorRef, id: Long) = {
    if (secondaries.size > 0) {
      unstashAll()
      val timer = context.system.scheduler.scheduleOnce(50 millis, self, Timeout)
      val replicators = secondaries.values.toSet
      context.become(globalAcknowledgement(op, replicators, actor, id, timer, 20))
    } else replyOperation(actor, OperationAck(id))
  }

  def globalAcknowledgement(op: Operation, expected: Set[ActorRef], actor: ActorRef, id: Long, timer: Cancellable, retries: Int): Receive = {
    case Replicated(_, _) => {
      val newExpected = expected - sender
      if (newExpected.size == 0) {
        timer.cancel
        replyOperation(actor, OperationAck(id))
      } else context.become(globalAcknowledgement(op, newExpected, actor, id, timer, retries))
    }
    case Replicas(replicas) => processReplicas(replicas)
    case Timeout => {
      val newExpected = expected.filter(secondaries.values.toList.contains(_))
      (newExpected.size, retries) match {
        case (0, _) => replyOperation(actor, OperationAck(id))
        case (_, 0) => replyOperation(actor, OperationFailed(id))
        case (n, r) => { 
          op match {
            case insert @ Insert(key, value, _) => for (r <- newExpected) r ! Replicate(key, Some(value), id)
            case remove @ Remove(key, _) => for (r <- newExpected) r ! Replicate(key, None, id)
            case _ =>
          }
          val newTimer = context.system.scheduler.scheduleOnce(50 millis, self, Timeout)
          context.become(globalAcknowledgement(op, newExpected, actor, id, newTimer, retries-1))
        }
      }
    }
    case msg => stash()
  }

  def getKey(key: String, id: Long, sender: ActorRef) = sender ! GetResult(key, kv.get(key), id)

  def replyOperation(actor: ActorRef, reply: OperationReply) = {
    actor ! reply
    unstashAll()
    context.become(leader)
  }

  def processReplicas(replicas: Set[ActorRef]) = {
    val newSecondaries = (replicas - self)
    secondaries.foreach(pair => if (!newSecondaries.contains(pair._1)) context.stop(pair._2))
    // newSecondaries.foreach(sec => secondaries.get(sec) match {case None => })
    secondaries = secondaries.filter(sec => newSecondaries.contains(sec._1))
    newSecondaries.foreach { sec =>
      val replicator = context.actorOf(Replicator.props(sec))
      secondaries += (sec -> replicator)
      kv.foreach(pair => replicator ! Replicate(pair._1, Some(pair._2), id))
    }
  }

}