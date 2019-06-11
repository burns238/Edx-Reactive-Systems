package kvstore

import akka.actor.Props
import akka.actor._
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.actor.Stash
import scala.language.postfixOps
import akka.util.Timeout

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long) extends Message
  case class SnapshotAck(key: String, seq: Long)

  case object ReSend

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with Stash {
  import Replicator._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valueOption, id) => replication(key, valueOption, id, sender)
  }

  def replication(key: String, valueOption: Option[String], id: Long, client: ActorRef) = {
    replica ! Snapshot(key, valueOption, _seqCounter)
    val timer = context.system.scheduler.scheduleOnce(50 millis, self, Timeout)
    context.become(waiting(key, valueOption, id, client, timer))
  }

  def waiting(key: String, valueOption: Option[String], id: Long, client: ActorRef, timer: Cancellable): Receive = {
    case SnapshotAck(key, seq) => {
      timer.cancel
      client ! Replicated(key, id)
      _seqCounter += 1
      unstashAll()
      context.become(receive)
    }
    case Timeout => replication(key, valueOption, id, client)
    case msg => stash()
  }

}
