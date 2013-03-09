package com.github.bigtoast.pakkxos

import concurrent.{ ExecutionContext, Future }
import akka.actor.{Address, ActorRef}


class KeyRegistrationException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)

  def this(cause: Throwable) = this(null, cause)

  override def toString = {
    Option(message).getOrElse("") + " " + Option(cause).map{ c => c + " " + c.getStackTraceString }.getOrElse("")
  }
}

/**
 * This indicates that a severe fatal error has occurred with the node mapping service and anything built on top
 * of the service should be rebuilt if it is keeping in sync with the service
 *
 * @param message
 * @param cause
 */
class FatalKeyRegistrationException(message :String, cause :Throwable) extends KeyRegistrationException(message, cause)

class ExistingKeyException(val key :String) extends KeyRegistrationException("Key: %s is already registered on a Node" format key)

trait KeyRegistrationEvent

case class KeyRegistration(key :String, address :Address, version :Long, createdAt :Long )

case class KeyRegistered( mapping :KeyRegistration ) extends KeyRegistrationEvent

case class KeyDeregistered( key :String, createdAt :Long ) extends KeyRegistrationEvent

/**
 * The distributed key registry is responsible for registering a key to a node and not
 * allowing a key to be registered on more than one node. It also provides hooks
 * to receive events regarding keys being registered / deregistered on different
 * nodes.
 */
trait DistributedKeyRegistry {

  /**
   * A mapping service is associated with a context.. essentially what do the keys mean.
   */
  def context: String

  //def ping :Status

  /**
   * Attempt to set the node associated with the key. If a node is already
   * registered with the key it will be returned instead and the registration
   * will fail. If the set is successful None will be returned.
   *
   * @return Some(Address) if registration exists | None if passed in node was set
   */
  @throws(classOf[FatalKeyRegistrationException])
  def getOrSet(key: String, node: Address): Future[KeyRegistration] // Option[Address]

  /**
   * Works just like getOrSet except that this implementation will try to set before
   * it tries to get.
   *
   */
  @throws(classOf[FatalKeyRegistrationException])
  def setOrGet(key: String, node: Address): Future[KeyRegistration] //Option[Address]

  /**
   * Remove the registration associated with the key matching the set version.
   * If the version passed in is -1 then there will be a force removal
   *
   * @return true if removal was successful.
   */
  @throws(classOf[FatalKeyRegistrationException])
  def remove(key: String, version :Long ): Future[Boolean]

  /**
   * Return the Address associated with the given key. If a node is not registered with
   * the key then None will be returned.
   *
   * @return Some[Address] if node exists | None if node doesn't exist
   */
  @throws(classOf[FatalKeyRegistrationException])
  def get(key: String): Future[Option[KeyRegistration]] //Option[Address]

  /**
   * Attempt to register the node with the key passed in. If the key is already registered
   * then an ExistingKeyException will be returned.
   *
   * @throws ExistingKeyException if the key is already registered.
   */
  @throws(classOf[ExistingKeyException])
  @throws(classOf[FatalKeyRegistrationException])
  def set(key: String, node: Address): Future[KeyRegistration]

  /** list current mappings */
  def list :Future[Seq[KeyRegistration]]

  /**
   * Clear all node mappings.
   */
  @throws(classOf[FatalKeyRegistrationException])
  def clear: Future[Unit]

  /**
   * Clear all registrations for the given Address.
   */
  @throws(classOf[FatalKeyRegistrationException])
  def clear(node: Address): Future[Unit]

  /**
   * Add a listener to receive AddressMappingEvents
   */
  def addListener(listener: ActorRef)

  /**
   * remove a listener. If the listener doesn't exist nothing will happen.
   */
  def removeListener(listener: ActorRef)

  /**
   * shutdown the service if it makes sense to do so.
   */
  def close()

  /** initialize the service if it makes sense to do so. */
  def init()

}
