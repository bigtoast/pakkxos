
package com.github.bigtoast.pakkxos

import akka.util.Timeout
import concurrent.duration._
import akka.pattern.ask
import akka.actor.{Address, ActorSystem, ActorRef}
import DistributedActorRegistryCommand._
import concurrent.Future


/** This provides clean access to a registry service and handles all the mapping
  * details.
  *
  * Keepers are special actors that can be clustered. A clustered keeper is a keeper
  * that will run on only one node in the cluster and all other nodes in the cluster
  * can send and receive messages from the keeper and it's children. The keeper is designed
  * to be a parent in a tree of actors.
  *
  * The registry acts as a keeper factory. There is one registry per node and a registry can
  * create an instance of a keeper or it can create references to remote keepers.
  *
  * In order to cleanly handle distributed messaging and cleanly handle situations where a
  * message to sent to a remote keeper that is not yet running, messages handled by top level
  * keepers should extends from some marker trait. The registry implementation must be able
  * to extract the key we cluster on from each message of this type.
  */
class DistributedActorRegistryService(registry :ActorRef, system :ActorSystem ){

  implicit val timeout = Timeout( system.settings.config.getInt("pakkxos.defaults.timeout_sec") seconds )

  import system.dispatcher

  /** return a keeper registration. This method is useful for debugging */
  def registration( key :String ) :Future[ActorRegistration] =
    ( registry ask GetRegistration( key ) ).mapTo[ActorRegistration]

  /** return the actor */
  def actor( key :String ) :Future[ActorRef] = registration(key).map(_.actor)

  /** check to see if this keeper is in sync with the cluster registrations. */
  def checkSync() :Future[Boolean] =
    ( registry ask CheckSync ).mapTo[Boolean]

  /** return the registrations on this node */
  def registered :Future[Map[String,ActorRegistration]] =
    ( registry ask GetRegistrations ).mapTo[Map[String,ActorRegistration]]

  /** return the just the keys for registrations on this node */
  def registeredKeys :Future[Seq[String]] =
    ( registry ask GetRegisteredKeys ).mapTo[Seq[String]]

  /** return just the keys for registrations across the cluster */
  def clusterRegisteredKeys :Future[Seq[String]] =
    ( registry ask GetClusterRegisteredKeys ).mapTo[Seq[String]]

  /** return all the registrations in the entire cluster sorted by the node containing the registrations */
  def clusterRegistrations :Future[Map[Address,Map[String,ActorRegistration]]] =
    ( registry ask GetClusterRegistrations ).mapTo[Map[Address,Map[String,ActorRegistration]]]

  /** this will blow up the registry which will trigger the registry to be rebuilt. Be very cautious with this method. */
  def bounce() :Future[Boolean] =
    ( registry ask BounceRegistry ).mapTo[Boolean]

  /** send a command to the keeper and expect a result of type R. The registry will resolve the keeper and forward the
    * command to it mapping the result to type R
    */
  def execute[R]( command :Any )( implicit m :Manifest[R] ) :Future[R] = {
    ( registry ask command ).mapTo[R]
  }

  /** dispatch a command the the keeper. This is fire n forget */
  def dispatch( command :Any ) = registry ! command

}

