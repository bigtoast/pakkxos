
package com.github.bigtoast.pakkxos

import language.postfixOps
import com.typesafe.config.ConfigFactory
import akka.remote.testkit.{MultiNodeSpecCallbacks, MultiNodeConfig, MultiNodeSpec}
import akka.testkit._
import scala.concurrent.duration._
import org.scalatest.{WordSpec, BeforeAndAfterAll}
import org.scalatest.matchers.MustMatchers
import akka.actor.{Props, Actor}
import akka.pattern.pipe
import akka.cluster.{MemberStatus, Member, Cluster}
import akka.cluster.ClusterEvent.{MemberEvent, CurrentClusterState, MemberUp, MemberDowned}

object SimpleDistActorSpecConfig extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")

  nodeConfig(node1)(ConfigFactory.parseString("akka.remote.netty.port = 5555"))
  nodeConfig(node2)(ConfigFactory.parseString("akka.remote.netty.port = 6666"))
  nodeConfig(node3)(ConfigFactory.parseString("akka.remote.netty.port = 7777"))

  commonConfig(ConfigFactory.parseString(
    """
      |akka {
      | loglevel = "INFO"
      |  actor {
      |    provider = "akka.cluster.ClusterActorRefProvider"
      |    debug {
      |      receive = on
      |      autoreceive = on
      |      lifecycle = on
      |      fsm = on
      |      event-stream = off
      |      unhandled = on
      |      router-misconfiguration = off
      |    }
      |  }
      |
      |  cluster {
      |    #seed-nodes = [
      |    #  "akka://SimpleDistActorSpec@127.0.0.1:5555",
      |    #  "akka://SimpleDistActorSpec@127.0.0.1:6666",
      |    #  "akka://SimpleDistActorSpec@127.0.0.1:7777"]
      |
      |    auto-join = off
      |  }
      |
      |  remote {
      |    transport = "akka.remote.netty.NettyRemoteTransport"
      |    log-remote-lifecycle-events = off
      |    netty {
      |      hostname = "127.0.0.1"
      |      #port = 0
      |    }
      |  }
      |
      |  extensions = ["akka.cluster.Cluster", "com.github.bigtoast.pakkxos.Pakkxos"]
      |
      |}
      |
      |pakkxos {
      |  lru_buffer_size = 1000
      |
      |  defaults {
      |    timeout_sec = 10
      |    mini_timeout_millis = 500
      |  }
      |
      |  key_manager {
      |    # a running protocol will be expired after this time in milliseconds
      |    protocol_timeout = 5000
      |
      |    # the manager will wait this long in seconds to receive a view of the cluster
      |    initialize_timeout = 30
      |  }
      |
      |  key_registration_retry_max = 3
      |
      |  registry_request_buffer_timeout_millis = 5000
      |
      |  distribution_extension = "com.github.bigtoast.pakkxos.PaxosActorDistribution"
      |
      |
      |}
    """.stripMargin))
}

trait MyMultiNodeSpec extends MultiNodeSpecCallbacks
  with WordSpec with MustMatchers with BeforeAndAfterAll {

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()
}

class MultiNodeSampleSpecMultiJvmNode1 extends SimpleDistActorSpec
class MultiNodeSampleSpecMultiJvmNode2 extends SimpleDistActorSpec
class MultiNodeSampleSpecMultiJvmNode3 extends SimpleDistActorSpec

class SimpleDistActorSpec extends MultiNodeSpec(SimpleDistActorSpecConfig)
  with MyMultiNodeSpec with ImplicitSender {

  import SimpleDistActorSpecConfig._
  import TestActorCommand._
  import system.dispatcher

  def initialParticipants = roles.size

  "Basic distributed actor functionality" must {
     /*
    "register a single actor across three nodes allow communication from all nodes" in {


      enterBarrier("init")

      Cluster(system).subscribe(testActor, classOf[MemberUp])

      expectMsgClass(classOf[CurrentClusterState])

      Cluster(system) join node(node1).address

      expectMsgAllOf[MemberUp]( 60 seconds,
        MemberUp(Member(node(node1).address, MemberStatus.Up)),
        MemberUp(Member(node(node2).address, MemberStatus.Up)),
        MemberUp(Member(node(node3).address, MemberStatus.Up)))

      enterBarrier("nodes-up")

      system.actorOf(Props[TestActorRegistry], name = "testRegistry")

      enterBarrier("registries-started")

      runOn(node1) {
        Pakkxos(system).registry("/user/testRegistry").execute[Sum](Add("666", 1)) pipeTo testActor
        expectMsgPF(1 second) {
          case msg :Sum =>
        }
      }

      runOn(node2) {
        Pakkxos(system).registry("/user/testRegistry").execute[Sum](Add("666", 3)) pipeTo testActor
        expectMsgPF(1 second) {
          case msg :Sum =>
        }
      }

      runOn(node3) {
        Pakkxos(system).registry("/user/testRegistry").execute[Sum](Add("666", 5)) pipeTo testActor
        expectMsgPF(1 second) {
          case msg :Sum =>
        }
      }

      enterBarrier("all-done")

      Pakkxos(system).registry("/user/testRegistry").execute[Sum](Add("666", 0)) pipeTo testActor
      expectMsg(Sum(9))

    } */

    "register an actor on one node, bring that node down and then register it on a new node" in {
      enterBarrier("init")

      Cluster(system).subscribe(testActor, classOf[MemberUp])

      expectMsgClass(classOf[CurrentClusterState])

      Cluster(system) join node(node1).address

      expectMsgAllOf[MemberUp]( 60 seconds,
        MemberUp(Member(node(node1).address, MemberStatus.Up)),
        MemberUp(Member(node(node2).address, MemberStatus.Up)),
        MemberUp(Member(node(node3).address, MemberStatus.Up)))

      enterBarrier("nodes-up")

      system.actorOf(Props[TestActorRegistry], name = "testRegistry")

      enterBarrier("registries-started")

      runOn(node1) {
        enterBarrier("start-actor")
        Pakkxos(system).registry("/user/testRegistry").execute[Sum](Add("666", 1)) pipeTo testActor
        expectMsgPF(1 second) {
          case msg :Sum =>
        }
      }

      runOn(node2) {

        Pakkxos(system).registry("/user/testRegistry").execute[Sum](Add("666", 3)) pipeTo testActor
        expectMsgPF(1 second) {
          case msg :Sum =>
        }
        enterBarrier("start-actor")
      }

      runOn(node3) {
        enterBarrier("start-actor")
        Pakkxos(system).registry("/user/testRegistry").execute[Sum](Add("666", 5)) pipeTo testActor
        expectMsgPF(1 second) {
          case msg :Sum =>
        }
      }

      enterBarrier("all-done")

      Pakkxos(system).registry("/user/testRegistry").execute[Sum](Add("666", 0)) pipeTo testActor
      expectMsg(Sum(9))

      enterBarrier("doit")

      //expectMsgPF(5 seconds ) {
        //case msg => println(s"\n\ngot msg ${msg}\n\n")
      //}

      //expectMsg(MemberDowned(Member(node(node1).address, MemberStatus.Down)))

      //println("\n\n SHUTTING DOWN 2 \n\n")

      runOn(node1) {
        Cluster(system).subscribe(testActor, classOf[MemberEvent])
        testConductor.removeNode(node2)
        //expectMsgClass(classOf[CurrentClusterState])
        //val addy = node(node1).address
        //Cluster(system).down(node(node1).address)
        Cluster(system).leave(node(node2).address)
        //testConductor.shutdown(node2,0).await
        expectMsgPF(10 seconds ) {
          case msg => println(s"\n\nSPECIAL 1 SPECIAL 1 ${msg}\n\n")
        }
        expectMsgPF(10 seconds ) {
          case msg => println(s"\n\nSPECIAL 2 SPECIAL 2 ${msg}\n\n")
        }
        //expectMsg( 5 seconds, MemberDowned(Member(addy, MemberStatus.Down)))
        Pakkxos(system).registry("/user/testRegistry").execute[Sum](Add("666", 11)) pipeTo testActor
        expectMsg(10 seconds, Sum(3))

      }

      enterBarrier("shizzz")
      println("\n\n DONEDONEDONEDONE \n\n")

    }

  }
}

sealed trait TestActorCommand
object TestActorCommand {
  case class Add(key :String, value :Int) extends TestActorCommand
}

case class Sum( value :Int )

class TestActor( key :String ) extends Actor {
  import TestActorCommand._

  var sum = 0

  def receive = {
    case Add( k, num ) =>
      sum = sum + num
      context.sender ! Sum(sum)
  }

}

class TestActorRegistry
  extends DistributedActorRegistry[TestActorCommand]
  with PickSelfStrategy {

  import TestActorCommand._

  def createProps( key :String ) = Props(new TestActor(key))

  keyResolver {
    case Add(k, _) => k
  }

}



