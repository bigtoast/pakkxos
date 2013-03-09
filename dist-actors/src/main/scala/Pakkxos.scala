package com.github.bigtoast.pakkxos

import akka.actor._
import akka.cluster.Cluster


/**
 * val reg = Pakkxos(system).registry("/user/distActors")
 *
 * // fire and forget
 * reg.dispatch("hello")
 *
 * // return a Future[String]
 * reg.execute[String]("nurse")
 *
 * reg.registration("12345")
 * reg.actor("12345")
 */

object Pakkxos extends ExtensionId[Pakkxos] with ExtensionIdProvider {

  override def lookup = Pakkxos

  override def createExtension(system: ExtendedActorSystem) = {
    val className = system.settings.config.getString("pakkxos.distribution_extension")
    val constructor = Class.forName(className).getConstructor(classOf[ExtendedActorSystem])
    constructor.newInstance( system ).asInstanceOf[Pakkxos]
  }

}

/** you need to implement this trait and the class constructor must take an ExtendedActorSystem as a parameter. */
abstract class Pakkxos( system :ExtendedActorSystem ) extends Extension {

  def factory :DistributedKeyRegistryFactory

  def registry( path :String ) = new DistributedActorRegistryService( system.actorFor(path), system )

}

class PaxosActorDistribution( system :ExtendedActorSystem ) extends Pakkxos(system) {

  lazy val factory = {
    val paxos = system.actorOf(Props( new PaxosDistributedKeyManager( Cluster(system).selfAddress ) ), name = "distributed-key-manager")
    new PaxosDistributedKeyRegistryFactory(system, paxos)
  }

  factory
}

/*
class LocalOnlyActorDistribution( system :ExtendedActorSystem ) extends DistributedActorExtension(system) {

  lazy val factory = new DistributedKeyRegistryFactory {
    def newService(context: String)( implicit ctx :ExecutionContext ): DistributedKeyRegistry =
      new LocalOnlyKeyRegistry( context, Diva.node, ctx )
  }

  lazy val clusterService = new SingleClusterManagerService( system )

}
*/


