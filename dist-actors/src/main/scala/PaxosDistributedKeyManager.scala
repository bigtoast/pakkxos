package com.github.bigtoast.pakkxos

import akka.actor._
import akka.actor.Status.Failure
import akka.pattern.{ ask, pipe }
import java.util.UUID
import concurrent.duration._
import akka.util.Timeout
import compat.Platform
import akka.cluster.ClusterEvent._
import akka.cluster.{MemberStatus, Cluster}
import akka.actor.Status.Failure
import scala.Some
import akka.actor.RootActorPath
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.LeaderChanged


object PaxosDistributedKeyManager {

  object ID {

    val LOW_MASK =  (0L | 0xffffffff)

    def apply( high :Int, low :Int ) = ( high.toLong << 32 ) | low

    def unapply( id :Long ) :Option[(Int,Int)] =
      Some(( id >>> 32 ).toInt, ( LOW_MASK & id ).toInt )

    def nextEpoch( id :Long ) = id match {
      case ID( epoch, counter ) => apply( epoch + 1 , 0 )
    }

  }

  sealed trait ProtocolMessage
  object ProtocolMessage {
    // ->
    case class Prepare( key :String, pNum :Long, protocolId :UUID ) extends ProtocolMessage

    // <-
    case class Promise( key :String, pNum :Long, protocolId :UUID ) extends ProtocolMessage

    // ->
    case class Accept(
      key        :String,
      pNum       :Long,
      cNum       :Long,
      protocolId :UUID,
      command    :Command,
      cTime      :Long = Platform.currentTime ) extends ProtocolMessage

    /* in case of dueling master's one should have an epoch out of sync. maybe this should extend Throwable */
    case class EpochOutOfSync( given :Int, current :Int ) extends ProtocolMessage

  }

  case object CheckForExpiredProtocols
  case object SyncTimeout

  case class AddListener( listener :ActorRef )
  case class RemoveListener( listener :ActorRef )

  sealed trait Request
  sealed trait Command extends Request { def key :String }
  object Request {
    case class Create( key :String, value :Address ) extends Command
    case class Get( key :String ) extends Request
    case object GetAll extends Request
    case class Update( key :String, value :Address, version :Long ) extends Command
    case class Delete( key :String, version :Long ) extends Command

    case object GetStatus
  }

  case class DataResponse( key :String, value :Address, stat :Stat )
  case class StatResponse( key :String, stat :Stat )
  case class DeletedResponse( key :String, value :Address, stat:Stat )
  case class NoSuchKey( key :String ) extends Throwable
  case class Stat( czxid :Long, mzxid :Long, version :Int, cTime :Long, mTime :Long )

  sealed trait ProtocolRole
  object ProtocolRole {
    case object INITIALIZING extends ProtocolRole
    case object PROPOSER_RUNNING extends ProtocolRole
    case object PROPOSER_SYNCING extends ProtocolRole
    case object ACCEPTOR_RUNNING extends ProtocolRole
    case object ACCEPTOR_SYNCING extends ProtocolRole
  }

  sealed trait ProtocolData
  object ProtocolData {

    /** collection representing clustered data. */
    type Accepted = Map[String,(Address,Stat)]

    case object Empty extends ProtocolData

    /** a leader protocol is either pending or proposing. If the protocol is pending, it means that another protocol
      * for the given key is running. We only run one protocol for a given key at a time.
      */
    sealed trait LeaderProtocol

    object LeaderProtocol {
      def apply( cmd :Command, initiator :ActorRef ) = ProtocolPending( cmd,  initiator )
    }

    case class ProtocolPending(
      /** commit this action on protocol completion */
      cmd :Command,

      /** send protcol results here */
      initiator :ActorRef,

      /** timestamp when the protocol started. */
      createdAt   :Long = Platform.currentTime,

      /** each protocol implementation gets a UUID for tracking n shiz */
      protocolId  :UUID = UUID.randomUUID ) extends LeaderProtocol

    /** Proposer side of the protocol */
    case class ProtocolPreparing(
      /** proposal number for the protocol */
      pNum        :Long,

      /** command requiring a protocol initialization */
      cmd         :Command,

      /** channel that spawned the protocol. protocol results sent here */
      initiator   :ActorRef,

      /** number of acceptors at time of protocol initialization. */
      acceptorCnt :Int,

      /** promises received from acceptors. When promises reach 1/2 acceptors + 1
        * the protocol can complete. */
      promises    :Set[ProtocolMessage.Promise] = Set.empty,

      /** timestamp when the protocol started. */
      createdAt   :Long = Platform.currentTime,

      /** each protocol implementation gets a UUID for tracking n shiz */
      protocolId  :UUID = UUID.randomUUID ) extends LeaderProtocol

    case class ProposerData(
      epoch  :Int,

      /** last proposal number issued */
      pNum   :Long,

      /** commit id */
      cNum   :Long,

      /** current view of the cluster */
      cluster   :CurrentClusterState,

      /** set of acceptor actor refs for the cluster */
      acceptors :Set[ActorRef] = Set.empty,

      /** accepted, clustered data set. */
      accepted  :Accepted      = Map.empty,

      /** running protocols. Only one protocol may run at a time for a key. If more
        * than one protocol command is issued for a key, the others must wait. */
      preparing :Map[String,ProtocolPreparing] = Map.empty,

      /** protocols which have not started yet because there is already a
        * protocol running for the given key. */
      pending   :Map[String,Vector[ProtocolPending]] = Map.empty,

      /** set of listeners to fire responses to */
      listeners :Set[ActorRef]  = Set.empty ) extends ProtocolData

    case class AcceptorData(
      epoch           :Int,
      highestAccepted :Long,
      proposer        :ActorRef,
      promised        :Map[String,Long] = Map.empty,
      accepted        :Accepted         = Map.empty,
      listeners       :Set[ActorRef]    = Set.empty ) extends ProtocolData

    case object Sync
    case class SyncData( epoch:Int, highest :Long, accepted :Accepted )
    case class KeyExists( key :String ) extends Throwable

  }

}

/** state machine to manage the clustered collection and protocol to maintain it.
  * This can act as a leader ( proposer ) or a follower ( acceptor ). Both the leader
  * and acceptor roles also act as learners. Additional learners could be added to this
  * without modification.
  *
  * TODO
  * purge stale protocols
  * handle competing protocols
  */
class PaxosDistributedKeyManager( me :Address ) extends Actor
  with LoggingFSM[PaxosDistributedKeyManager.ProtocolRole,PaxosDistributedKeyManager.ProtocolData]
  with LRUMessageBuffer {

  import PaxosDistributedKeyManager._
  import ProtocolRole._
  import ProtocolData._
  import Request._
  import ProtocolMessage._
  import context.dispatcher

  implicit val to = Timeout( context.system.settings.config.getInt("pakkxos.defaults.timeout_sec") seconds )

  val protocolTimeout   = context.system.settings.config.getLong("pakkxos.key_manager.protocol_timeout")
  val initializeTimeout = context.system.settings.config.getLong("pakkxos.key_manager.initialize_timeout") seconds

  val clusterService = Cluster(context.system)

  startWith(INITIALIZING, Empty)

  override def preStart(){
    log.debug("Starting PaxosDistributedKeyManager.. Waiting for CurrentView of cluster.")
    //clusterService.subscribe(context.self, classOf[CurrentClusterState] )
    clusterService.subscribe(context.self, classOf[LeaderChanged])
    clusterService.subscribe(context.self, classOf[MemberEvent])
    //clusterService.sendCurrentClusterState(context.self)
  }

  override def postStop(){
    clusterService.unsubscribe(context.self)
  }

  def makeRef( address :Address ) =
    context.actorFor( RootActorPath(address) / context.self.path.elements )

  def createAcceptors( cluster :CurrentClusterState ) =
    cluster.members
      .filter(_.status == MemberStatus.up)
      .filterNot(_.address == me)
      .map( m => makeRef(m.address) )

  /** handle protocol commands and initiate a protocol instance when needed.
    * This method has side effects because it may call 'initiate'. The changes
    * made to ProposerData are not persisted here. The return value needs to be
    * retained.
    */
  def handle( protocol :ProtocolPending, data :ProposerData ) :ProposerData = data.preparing.contains(protocol.cmd.key) match {
    case true =>
      data.copy(
        pending = data.pending.updated( protocol.cmd.key,
          data.pending.get(protocol.cmd.key).getOrElse(Vector.empty) :+ protocol ) )

    case false =>
      protocol.cmd match {
        case c :Create  =>
          if ( data.accepted.contains(c.key) ) {
            protocol.initiator ! Failure( KeyExists( c.key ) )
            checkStartPending(protocol.cmd.key,data)
          } else {
            initiate( protocol, data )
          }

        case c :Update  =>
          if ( data.accepted.contains(c.key) ) {
            initiate( protocol, data)
          } else {
            protocol.initiator ! Failure( NoSuchKey(c.key) )
            checkStartPending(protocol.cmd.key,data)
            data
          }

        case c :Delete =>
          if ( data.accepted.contains(c.key) ) {
            initiate( protocol, data)
          } else {
            protocol.initiator ! Failure( NoSuchKey(c.key) )
            checkStartPending(protocol.cmd.key,data)
            data
          }
      }
  }

  /** initiate the protocol by sending prepare requests and putting a LeaderProtocol
    * object in the data.preparing collection. If there are no acceptors running,
    * meaning there is only one server running the protocol will be committed
    * directly.
    */
  def initiate( protocol :ProtocolPending, data :ProposerData ) :ProposerData = {
    val prepare = ProtocolPreparing(
      pNum        = data.pNum + 1,
      cmd         = protocol.cmd,
      initiator   = protocol.initiator,
      acceptorCnt = data.acceptors.size,
      createdAt   = protocol.createdAt,
      protocolId  = protocol.protocolId )

    val updated = data.copy( pNum = data.pNum + 1, preparing = data.preparing + ( protocol.cmd.key -> prepare ) )

    if ( data.acceptors.size == 0 ) {
       commit( prepare, updated )
    } else {
      val prepare = Prepare( protocol.cmd.key, data.pNum + 1, UUID.randomUUID )
      data.acceptors.foreach { _ ! prepare }
      updated
    }
  }

  /** handle a received promise when a leader. */
  def handle( promise :Promise, data :ProposerData ) :ProposerData = {
    data.preparing.get(promise.key) match {
      case Some( prot ) =>
        val updated = prot.copy( promises = prot.promises + promise )

        // the leader is an acceptor as well
        if ( updated.promises.size + 1 > ( ( updated.acceptorCnt + 1 ) / 2 ) ) { // accepted
           commit( prot, data )
        } else {
           data.copy( preparing = data.preparing + ( promise.key -> updated ) )
        }

      case None =>
        log.debug("Leader dropping promise {} with no matching protocol",promise)
        data

    }
  }

  /** commit the command and finish the protocol. This will fire accept to all
    * acceptors. If there are any pending protocols the first one will be started.
    * @todo we should fire a change event
    */
  def commit( protocol :ProtocolPreparing, data :ProposerData ) :ProposerData = {
    val updated = protocol.cmd match {
      case c :Create =>
        val now    = Platform.currentTime
        val accept = Accept( protocol.cmd.key, protocol.pNum, data.cNum + 1, protocol.protocolId, protocol.cmd, now )
        val stat   = Stat(protocol.pNum, data.cNum + 1, 0, now, now)

        data.acceptors.foreach { _ ! accept }

        val dResp = DataResponse( c.key, c.value, stat )
        log.debug("Committing {} responding to {}", dResp, protocol.initiator )
        protocol.initiator ! dResp
        data.listeners.foreach( _ ! dResp )

        data.copy(
          accepted  = data.accepted + ( c.key -> (c.value, stat ) ),
          preparing = data.preparing - c.key,
          cNum      = data.cNum + 1
        )

      case c :Update => // update correct stat
        val now    = Platform.currentTime
        val ( _, oldStat ) = data.accepted(c.key)
        val accept = Accept( protocol.cmd.key, protocol.pNum, data.cNum + 1, protocol.protocolId, protocol.cmd, now )
        val stat   = oldStat.copy( mzxid = data.cNum + 1, mTime = now, version = oldStat.version + 1  )

        data.acceptors.foreach { _ ! accept }

        val dResp = DataResponse( c.key, c.value, stat )
        protocol.initiator ! dResp
        data.listeners.foreach( _ ! dResp )

        data.copy(
          accepted  = data.accepted + ( c.key -> (c.value, stat ) ),
          preparing = data.preparing - c.key,
          cNum      = data.cNum + 1
        )

      case c :Delete => // update correct stat
        val now    = Platform.currentTime
        val ( value , oldStat ) = data.accepted(c.key)
        val accept = Accept( protocol.cmd.key, protocol.pNum, data.cNum + 1, protocol.protocolId, protocol.cmd, now )

        data.acceptors.foreach { _ ! accept }

        val dResp = DeletedResponse( c.key, value, oldStat )
        protocol.initiator ! dResp
        data.listeners.foreach( _ ! dResp )

        log.debug("KeyManager leader committing delete {}", c)
        data.copy(
          accepted  = data.accepted - c.key,
          preparing = data.preparing - c.key,
          cNum      = data.cNum
        )

      case c :Get => // do nothing.. actually we should never get here
        data
    }

    // if there are any pending protocols.. fire 'em off
    checkStartPending(protocol.cmd.key, updated)
  }

  /** check to see if there is a pending protocol to start. If there is then start it. */
  def checkStartPending( key :String, data :ProposerData ) :ProposerData = {
    if ( data.pending.contains(key) ) {
      handle(
        data.pending(key).head,
        data.copy( pending =
          data.pending.updated( key, data.pending(key).tail ).filter{ case (str, vctr) => vctr.nonEmpty } ) )
    } else {
      data
    }
  }

  /** Commit an accept message. This does not check if commit is called for,
    * it just commits the data.
    */
  def commit( accept :Accept, data :AcceptorData ) :AcceptorData = {
    accept.command match {
      case c :Create =>
        val stat = Stat( accept.cNum, accept.cNum, 0, accept.cTime, accept.cTime )
        val updated = data.copy(
          accepted = data.accepted + ( accept.key -> ( c.value, stat )  ),
          promised = data.promised -  accept.key // check pNum.. what about dueling promises
        )

        data.listeners.foreach { _ ! DataResponse( c.key, c.value, stat ) }

        updated

      case c :Update =>
        val ( _, oldStat ) = data.accepted(c.key)
        val newStat = oldStat.copy( mzxid = accept.cNum, mTime = accept.cTime, version = oldStat.version + 1  )
        val updated = data.copy(
          accepted = data.accepted + ( accept.key -> ( c.value, newStat ) ),
          promised = data.promised -  accept.key // check pNum.. what about dueling promises
        )

        data.listeners.foreach { _ ! DataResponse( c.key, c.value, newStat ) }

        updated

      case c :Delete =>
        val ( value , oldStat) = data.accepted(c.key)
        data.listeners.foreach { _ ! DeletedResponse( c.key, value, oldStat ) }

        log.debug("DNM follower committing delete {}", c)
        data.copy(
          accepted = data.accepted - accept.key,
          promised = data.promised - accept.key // check pNum.. what about dueling promises
        )

      case c :Get    =>
        data

    }
  }

  when(INITIALIZING, initializeTimeout ) {
    case Event( cluster :CurrentClusterState, Empty ) if cluster.leader.isDefined =>
    //case Event( ClusterView( ClusterState.MEMBER, Some( cluster ) ), Empty ) =>
      if ( cluster.leader.get == me ) {
        val acceptors = createAcceptors( cluster )
        val zxid = ID(0,0)
        goto( PROPOSER_RUNNING ) using( ProposerData(0, zxid, zxid, cluster, acceptors ) )
      } else {
        val proposer = makeRef(cluster.leader.get)
        proposer ! Sync
        goto( ACCEPTOR_SYNCING ) using( AcceptorData(0, ID(0,0), proposer ) )
      }

    case Event( cluster :CurrentClusterState, Empty ) =>
      log.warning("{} Could not initialize DistributedKeyManager because this node is not yet a member. Current State: {}. Retrying Init...",me, cluster )
      val meRef = context.self
      context.system.scheduler.scheduleOnce(200 millis){
        clusterService.sendCurrentClusterState(meRef)
      }
      stay()

    case Event( e :MemberEvent, Empty ) =>
      clusterService.sendCurrentClusterState(context.self)
      stay

    case Event( e :LeaderChanged, Empty ) =>
      clusterService.sendCurrentClusterState(context.self)
      stay

    case Event( FSM.StateTimeout, _ ) =>
      log.error("{} timed out while initializing after 30 seconds", me)
      throw new RuntimeException("timed out while initializing after 30 seconds")

  }

  when(PROPOSER_RUNNING) {
    case Event( Sync, data :ProposerData ) =>
      log.debug("Proposer received sync request from {}. Issuing {}", context.sender, data.pNum)
      context.sender ! SyncData(data.epoch, data.pNum , data.accepted )
      stay()

    case Event( req :Get, data :ProposerData ) =>
      data.accepted.get(req.key) match {
        case Some( ( bytes, stat) ) =>
          context.sender ! DataResponse(req.key, bytes, stat )
        case None =>
          context.sender ! Failure(NoSuchKey(req.key))
      }
      stay()

    case Event( GetAll, data :ProposerData ) =>
      context.sender ! data.accepted.map { case ( key, ( node, stat )) =>
        DataResponse( key, node, stat )
      }.toSeq

      stay()

    case Event( cmd :Command, data :ProposerData ) =>
      val updated = handle( ProtocolPending(cmd, context.sender) , data )
      stay using updated

    case Event( msg :Promise, data :ProposerData ) =>
      val updated = handle(msg, data)
      stay using updated

    case Event( AddListener( listener ), data :ProposerData ) =>
      stay using data.copy( listeners = data.listeners + listener )

    case Event( RemoveListener( listener ), data :ProposerData ) =>
      stay using data.copy( listeners = data.listeners - listener )

    case Event( prepare :Prepare, data :ProposerData ) =>
      // big error this shouldn't happen
      prepare.pNum match {
        case ID( epoch , _ ) if epoch < data.epoch => // they are out of sync
          stay() replying( EpochOutOfSync(epoch, data.epoch) )

        case ID( epoch, _ ) =>
          log.error("Dueling Proposers found. Recieved {} from {} but my epoch is {}. I think I'm out of sync. Reinitializing.", prepare, context.sender, data.epoch)
          clusterService.sendCurrentClusterState(context.self)
          goto(INITIALIZING) using(Empty)
      }

    case Event( EpochOutOfSync( mine, current), data :ProposerData ) =>
      if ( current != data.epoch ) {
        log.error("My epoch is out of sync. Sent {} but my epoch is {}. Reinitializing.", current, data.epoch)
        clusterService.sendCurrentClusterState(context.self)
        goto(INITIALIZING) using(Empty)
      } else {
        stay()
      }

    case Event( LeaderChanged( Some(addy) ), data :ProposerData ) if addy != me =>
        val proposer = makeRef(addy)
        proposer ! Sync
        goto( ACCEPTOR_SYNCING ) using( AcceptorData( 0, 0, proposer ) )

    case Event( e :MemberEvent, data :ProposerData ) =>
      clusterService.sendCurrentClusterState(context.self)
      stay

    case Event( cluster : CurrentClusterState, data :ProposerData ) =>
      cluster.leader match {
        case Some( addy ) if addy == me =>
          val acceptors = createAcceptors( cluster )
          stay using data.copy( cluster = cluster, acceptors = acceptors )
        case Some( addy ) =>
          // no longer leader
          val proposer = makeRef(addy)
          proposer ! Sync
          goto( ACCEPTOR_SYNCING ) using( AcceptorData( 0, 0, proposer ) )
        case None =>
          stay() // what to do .. can this even happen?

      }

    case Event( CheckForExpiredProtocols, data :ProposerData ) =>
      val now = Platform.currentTime
      val ( good, expired ) = data.preparing.partition{ case ( key, protocol ) =>
        ( now - protocol.createdAt ) <= protocolTimeout }

      if ( expired.isEmpty ) {
        stay()
      } else {
        log.warning("Expiring expired ( > {} millis ) protocols {}", protocolTimeout, expired )
        //Diva.stats.incr("dnm-protocol.expired", expired.size )
        stay using data.copy( preparing = good )
      }

  }

  when(ACCEPTOR_SYNCING){
    case Event( SyncData( epoch, highest, accepted ), data :AcceptorData  ) =>
      goto(ACCEPTOR_RUNNING) using( data.copy( epoch = epoch, highestAccepted = highest, accepted = accepted ) )

    case Event( SyncTimeout, data :AcceptorData ) =>
      data.proposer ! Sync
      goto(ACCEPTOR_SYNCING) using(data)

    case Event( Failure( err :UninitializedError ), data :AcceptorData ) =>
      log.warning("{} Received UninitializedError while syncing acceptor. Retrying after 100 millis", me)
      context.system.scheduler.scheduleOnce(100 millis) {
        data.proposer ! Sync
      }
      stay()
  }

  when(ACCEPTOR_RUNNING){
    case Event( cmd :Get, data :AcceptorData ) =>
      data.accepted.get(cmd.key) match {
        case Some( ( bytes, stat ) ) =>
          context.sender ! DataResponse( cmd.key, bytes, stat )

        case None =>
          context.sender ! Failure(NoSuchKey(cmd.key))
      }
      stay()

    case Event( GetAll, data :AcceptorData ) =>
      context.sender ! data.accepted.map { case ( key, ( node, stat )) =>
        DataResponse( key, node, stat )
      }.toSeq

      stay()

    case Event( cmd :Command , data :AcceptorData ) =>
      log.debug("Forwarding {} to proposer {}", cmd, data.proposer)
      data.proposer forward cmd
      stay()

    case Event( msg :Prepare , data :AcceptorData ) if data.promised.contains(msg.key) =>
      // options.. no promise, already promised,
      // @todo what do we do???
      stay()

    case Event( msg :Prepare, data :AcceptorData ) =>
      // no promise
      data.accepted.get(msg.key) match {
        case Some( ( bytes, stat ) ) if stat.mzxid >= msg.pNum =>
          // failure
          stay()
        case _ =>
          val promise = Promise( msg.key, msg.pNum, msg.protocolId )
          val updated = data.copy( promised = data.promised + ( msg.key -> msg.pNum ) )
          stay using updated replying promise
      }

    case Event( msg :Accept, data :AcceptorData ) =>
      val updated = commit( msg, data )
      stay using updated

    case Event( AddListener( listener ), data :AcceptorData ) =>
      stay using data.copy( listeners = data.listeners + listener )

    case Event( RemoveListener( listener ), data :AcceptorData ) =>
      stay using data.copy( listeners = data.listeners - listener )

    case Event( LeaderChanged( Some(addy) ), data :AcceptorData ) =>
      clusterService.sendCurrentClusterState(context.self)
      stay

    case Event( cluster :CurrentClusterState, data :AcceptorData ) =>
      cluster.leader match {
        case Some( addy ) if addy == me =>
          val next = data.epoch + 1
          val proposerData = ProposerData(
            epoch     = next,
            pNum      = next,
            cNum      = next,
            cluster   = cluster,
            acceptors = cluster.members.filter(_.status == MemberStatus.Up ).map(m => makeRef(m.address) ),
            accepted  = data.accepted ) // what to do about followers.

          goto(PROPOSER_RUNNING) using(proposerData)

        case Some( addy ) =>
          val leader = makeRef( addy )
          stay() using data.copy( proposer = leader )

        case None =>
          stay // what to do
      }

  }

  whenUnhandled {
    case Event(cmd :AddListener, _ ) =>
      context.system.scheduler.scheduleOnce(100 milliseconds, context.self, cmd )
      stay()

    case Event(cmd :RemoveListener, _ ) =>
      context.system.scheduler.scheduleOnce(100 milliseconds, context.self, cmd )
      stay()

    case Event( GetStatus, _ ) =>
      context.sender ! this.stateName
      stay()

    case Event( bufferMe, _ )  =>

      stateName match {
        case PROPOSER_RUNNING | ACCEPTOR_RUNNING =>
          log.warning("Received msg {} in state {}. Not buffering.", bufferMe, stateName)
        case _ =>
          // Add handling so we don't buffer messages when in a running state.. although we
          // should log them at a level above debug.
          log.warning("BUFFERING msg {} in state {}.", bufferMe, stateName)
          buffer( bufferMe, context.sender )
      }

      stay()


    case e =>
      log.warning("DNM got unhandled {}", e)
      stay()
  }

  onTransition {

    case t @ (PROPOSER_RUNNING ->  ACCEPTOR_SYNCING) =>
      setTimer("sync-timeout", SyncTimeout, 250 millis, false )
      cancelTimer("expired-protocol-check")
      log.debug("{} ! Paxos making transition {}", me, t)

    case t @ ( any -> ACCEPTOR_SYNCING) =>
      log.debug("{} ! Paxos making transition {}", me, t)
      setTimer("sync-timeout", SyncTimeout, 250 millis, false )

    case t @ ( any -> PROPOSER_RUNNING ) =>
      flush()
      setTimer("expired-protocol-check", CheckForExpiredProtocols, 5 second, true )
      log.debug("{} ! Paxos making transition {}", me, t)

    case t @ ( any -> ACCEPTOR_RUNNING ) =>
      flush()
      log.debug("{} Paxos making transition {}", me, t)

    case t =>
      log.debug("{} Paxos making transition {}", me, t)

  }

  initialize
}





