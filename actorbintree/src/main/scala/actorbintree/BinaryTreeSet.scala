/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  case class Copy(newRoot: ActorRef)

  case object CopyComplete

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with Stash {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case i @ Insert(_, _, _) => root ! i
    case r @ Remove(_, _, _) => root ! r
    case c @ Contains(_, _, _) => root ! c
    case GC => {
      val newRoot = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))
      root ! Copy(newRoot)
      context.become(garbageCollecting(newRoot))
    }
    case msg => stash()
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyComplete => {
      root = newRoot
      unstashAll()
      context.unbecome()
    }
    case GC => 
    case msg => stash()
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  def createSubtree(elem: Int, pos: Position) = {
    val actorRef = context.actorOf(BinaryTreeNode.props(elem, false))
    this.subtrees += (pos -> actorRef)
  }

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {     
    case op @ Insert(_, _, _) => process(op)
    case op @ Remove(_, _, _) => process(op)
    case op @ Contains(_, _, _) => process(op)
    case Copy(newRoot) => copy(newRoot)
  }

  def process(operation: Operation) = {
    (operation.elem, subtrees.get(Left), subtrees.get(Right)) match {
      case (e, _, _) if e == this.elem => processMatch(operation)
      case (e, _, Some(r)) if e > this.elem => r ! operation
      case (e, _, None) if e > this.elem => processEmptySubtree(Right, operation)
      case (e, Some(l), _) if e < this.elem => l ! operation
      case (e, None, _) if e < this.elem => processEmptySubtree(Left, operation)
    }
  }

  def processMatch(operation: Operation) = {
    operation match {
      case Insert(requester, id, _) => {
        this.removed = false
        requester ! OperationFinished(id)
      }
      case Remove(requester, id, _) => {
        this.removed = true
        requester ! OperationFinished(id)
      }
      case Contains(requester, id, elem) => requester ! ContainsResult(id, this.removed == false)
    }
  }

  def processEmptySubtree(position: Position, operation: Operation) = {
    operation match {
      case Insert(requester, id, e) => {
        createSubtree(e, position)
        requester ! OperationFinished(id)
      }
      case Remove(requester, id, _) => requester ! OperationFinished(id)
      case Contains(requester, id, _) => requester ! ContainsResult(id, false)
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(this.elem) => checkCopyComplete(expected, true)
    case CopyComplete => checkCopyComplete(expected - sender, insertConfirmed)
  }

  def copy(newRoot: ActorRef) = {
    if (!this.removed) {
      newRoot ! Insert(self, this.elem, this.elem)
    }
    val children = subtrees.values.toSet
    children.foreach(_ ! Copy(newRoot))
    checkCopyComplete(children, this.removed)
  }

  def checkCopyComplete(expected: Set[ActorRef], insertConfirmed: Boolean) = {
    if (expected.size == 0 && insertConfirmed) {
      context.parent ! CopyComplete
      context.stop(self)
    } else {
      context.become(copying(expected, insertConfirmed))
    }
  }

}
