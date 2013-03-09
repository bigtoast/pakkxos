package com.github.bigtoast.pakkxos

import akka.actor.{ ActorRef, ActorSystem }
import concurrent.ExecutionContext

trait HasDistributedKeyRegistryFactory {
  def nodeMappingServiceFactory :DistributedKeyRegistryFactory
}

trait DistributedKeyRegistryFactory {
  def newService( context :String )( implicit cxt :ExecutionContext ) :DistributedKeyRegistry
}

class PaxosDistributedKeyRegistryFactory( system :ActorSystem, paxos :ActorRef ) extends DistributedKeyRegistryFactory {
  def newService( context :String )( implicit ctx :ExecutionContext ) =
    new PaxosDistributedKeyRegistry( context, paxos, system )
}

/*

class AsyncZKDistributedKeyRegistryFactory( zk :AsyncZooKeeperClient ) extends DistributedKeyRegistryFactory {
  def newService( context :String )( implicit ctx :ExecutionContext ) =
    new AsyncZooKeeperDistributedKeyRegistry( context, zk, ctx )
}

@deprecated(since = "44.0", message = "use async zk service")
trait HasZooKeeperDistributedKeyRegistryFactory extends HasDistributedKeyRegistryFactory {

  val nodeMappingServiceFactory = new DistributedKeyRegistryFactory {

    def newService(context: String)( implicit ctx :ExecutionContext ): DistributedKeyRegistry = {
      val service =  new ZooKeeperDistributedKeyRegistry(
        context,
        Diva.node,
        Diva.config.getString("diva.zookeeper.connect_string"),
        Diva.config.getInt("diva.zookeeper.session_timeout"),
        str => str,
        ctx )

      service.init
      service
    }

  }

}

trait HasLocalOnlyNodeKeyRegistryFactory extends HasDistributedKeyRegistryFactory {

  val nodeMappingServiceFactory = new DistributedKeyRegistryFactory {
    def newService(context: String)( implicit ctx :ExecutionContext ): DistributedKeyRegistry =
      new LocalOnlyKeyRegistry( context, Diva.node, ctx )
  }

}

*/