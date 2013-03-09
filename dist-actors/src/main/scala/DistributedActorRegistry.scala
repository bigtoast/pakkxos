package com.github.bigtoast.pakkxos

import akka.actor._
import akka.pattern.{ask, pipe}
import reflect.ClassTag
import akka.util.Timeout
import concurrent.duration._
import akka.cluster.{MemberStatus, Cluster}
import akka.actor.Status.Failure
import compat.Platform
import concurrent.{Await, Promise, Future}
import akka.cluster.ClusterEvent.{MemberEvent, ClusterDomainEvent, CurrentClusterState}
import scala.collection.mutable
import akka.event.{LoggingReceive, LoggingAdapter}

/** Keeper commands are internal commands */
sealed trait DistributedActorRegistryCommand
object DistributedActorRegistryCommand {

  /** return the local and remote registration from one node */
  case object GetRegistrations extends DistributedActorRegistryCommand

  /** return just the keys for the registrations on this node. */
  case object GetRegisteredKeys extends DistributedActorRegistryCommand

  /** return just the keys for all registrations in the cluster */
  case object GetClusterRegisteredKeys extends DistributedActorRegistryCommand

  /** return registrations from all nodes in the cluster */
  case object  GetClusterRegistrations extends DistributedActorRegistryCommand

  /** check if a keepers are in sync with node registry. If they are not, blow up the keeper and restart. */
  case object CheckSync extends DistributedActorRegistryCommand

  /** return a keeper registration for a key. If assign is true the node receiving the
    * request will be assigned the registration if the key is not already registered. This
    * flag is primarily used for internal use. */
  case class  GetRegistration( key :String, assign :Boolean = false ) extends DistributedActorRegistryCommand

  /** manual trigger to blow up the registry and recover. don't use this unless you have to. */
  case object BounceRegistry extends DistributedActorRegistryCommand

}

case class ActorRegistration( mapping :KeyRegistration, actor :ActorRef )
case class ActorStopped( mapping :KeyRegistration )
case class ActorStarted( reg :ActorRegistration )
case class ActorRegistryException( msg :String, cause :Throwable ) extends RuntimeException(msg, cause)
case class ActorRequestBufferTimeoutException( msg :String ) extends RuntimeException(msg)
case class ActorRegistryOutOfSyncException() extends RuntimeException

/**
 * A keeper registry registers running keepers ( actor refs ) with a key. The registry
 * will take care of calling context.watch on newly created keepers and properly
 * handling the Terminated messages when keepers are stopped. In order to handle the
 * Terminated message the actor's receive block should be wrapped in RegistryReceive.
 *
 * This should handle messages of type M and be able to extract a key from
 * the message of type K. This key is used for registering keepers
 *
 */
abstract class DistributedActorRegistry[M]( implicit m :ClassTag[M] ) extends Actor with ActorLogging {

  self :DistributionStrategy =>

  import DistributedActorRegistryCommand._

  val pakkxos = Pakkxos(context.system)
  val clusterExtension = Cluster(context.system)

  val me  = clusterExtension.selfAddress
  val fme = Future.successful(me)

  import context.dispatcher

  implicit val l = log

  private[this] val config = context.system.settings.config

  private[this] implicit val timeout = Timeout( config.getInt("pakkxos.defaults.timeout_sec") seconds )

  private[this] lazy val regRetryMax = config.getInt("pakkxos.key_registration_retry_max")

  private[this] lazy val requestBufferMax = config.getLong("pakkxos.registry_request_buffer_timeout_millis")

  private[this] lazy val mClazz :Option[Class[_]] = Some( m.runtimeClass )

  context.system.eventStream.subscribe( context.self, classOf[DeadLetter]   )

  private[this] var keyRegistry :DistributedKeyRegistry = _

  private[this] var _registries :Map[Address,ActorRef] = Map.empty[Address,ActorRef]

  final override def preStart() :Unit = {
    //log.debug("Starting keeper %s waiting for cluster view" format context.self)
    clusterExtension.subscribe(context.self, classOf[ClusterDomainEvent])
    //clusterExtension.subscribe(context.self, classOf[MemberEvent] )
    //clusterExtension.sendCurrentClusterState(context.self)
    //_cluster = Await.result( (clusterManager ask CurrentView).mapTo[ClusterView], Diva.config.getInt("pakkxos.defaults.mini_timeout_millis") millis ).cluster.get // change to no stable view of the cluster and blow up
    //log.debug("Starting keeper %s recieved view %s of cluster. Getting NMS..." format( context.self, _cluster ))
    //_hash = makeHash(_cluster)
    keyRegistry = pakkxos.factory.newService(context.self.path.elements.mkString("-"))
    keyRegistry.addListener( context.self )
    log.debug(s"ActorRegistry ${context.self} started")
    context.system.scheduler.schedule(requestBufferMax milliseconds,requestBufferMax milliseconds, context.self, FlushRequestBuffer(false) )
  }

  final override def preRestart( cause :Throwable, message :Option[Any] ) {
    context.children.foreach { _ ! PoisonPill }
    log.error(cause, "Restarting Keeper {} due to error.", context.self.path )
    //Diva.stats.incr(registryName + ".restarted")
  }

  final override def postStop(){
    println(s"\n\n STOPPING ACTOR REGISTRY ${context.self} \n\n")
    try {
      clusterExtension.unsubscribe(context.self)
      keyRegistry.removeListener( context.self )
      keyRegistry.close()
    } catch {
      case e :Throwable =>
        log.warning("Exception {} in DistributedActorRegistry.postStop {}",e, context.self)
    }
  }

  /**
   * Extract a key from an ActorPath. By default we just take the last
   * element in the path
   */
  def pathToKey( path :ActorPath ) :String  = path.elements.last

  private[this] var _activeKeyResolver :PartialFunction[M,String] = Map.empty

  /**
   * based on a given message. if the message is intended to go to a clustered
   * entity extract the entity id of from the message.
   *
   * This should match on the passed in message and if the message matches a clustered
   * entity the key of type K should be extracted by the entity.
   *
   * messages caught on the dead letter queue are matched using this also in order
   * to not lose messages
   *
   */
  def keyResolver( pFunc :PartialFunction[M,String] ) = _activeKeyResolver = pFunc

  private[this] var _receive :Receive = { case _ => } // do nothing.. drop all

  private[this] lazy val doRec = new PartialFunction[Any,Unit]{
    override def isDefinedAt( msg  :Any ) = _receive.isDefinedAt(msg)
    override def apply(msg :Any) = _receive.apply(msg)
  }

  /**
   * call this method to register normal message handling for commands requesting things other that a keeper.
   */
  def doReceive( block : Receive ) = _receive = block

  /**
   * Create a keeper for a given key. This keeper needs to be created with
   * the actor system associated with this registry.
   */
  def createProps( key :String ) :Props

  def createActor( key :String ) :ActorRef =
    context.actorOf( createProps(key), name = key )

  /**
   * Create a remote actor ref for the given key and node.
   */
  def getRemoteActorRef( key :String, address :Address ) :ActorRef = {
    val path = RootActorPath( address ) / context.self.path.elements / key
    context.actorFor(path)
  }

  private[this] val _keepers = new mutable.HashMap[String,ActorRegistration]

  /** key -> ( ts when added to buffer, buffered action ) */
  private[this] var _requestBuffer = Map.empty[String,Seq[(Long, ChannelAction)]]

  /**
    * Returns an immutable map of the [K,Keeper]
    */
  def keepers = _keepers.toMap

  /**
    * just for internal uses
    */
  private[this] case class NodeRegistration( key :String, mapping :KeyRegistration, action :ChannelAction , startedAt :Long )
  private[this] case class RemoteActorRegistration( reg :ActorRegistration, action :ChannelAction )
  private[this] case class NodeRegistrationError(key :String, action :ChannelAction, attempt :Int, error :Throwable )

  /** internal message for flushing the request buffer. If force is false, only requests that have been buffered longer
    * than registry_request_buffer_timeout_millis will be flushed. If force is true, all requests will be flushed.
    */
  private[this] case class FlushRequestBuffer( force :Boolean )

  /** indicate what to do with the keeper once it is acquired. We can either
    * reply, send the keeper to the channel, or we can forward a message to the
    * keeper
    */
  private[this] sealed trait ChannelAction {

    def channel :ActorRef

    def handle( reg :ActorRegistration )( implicit l :LoggingAdapter ) = try {
      this match {
        case Forward( message, channel ) =>

          reg.actor.tell(message, channel )

        case Reply( channel ) =>
          channel ! reg
      }
    } catch {
      case e :Throwable =>
        l.error(e , "Exception handing channel action {}", this)
    }

    def error( e :Throwable )( implicit l :LoggingAdapter ) = try {
      channel ! Failure( e )
      log.warning("Returning error {} from ChannelAction to channel {}", e, channel )
    } catch {
      case e :Throwable =>
        l.error(e, "Exception handinging channel error {} in action {}",e , this)
    }

  }

  private[this] case class Forward(  message :Any, channel :ActorRef ) extends ChannelAction
  private[this] case class Reply( channel :ActorRef ) extends ChannelAction

  /** check to see if the local cache of registrations is in sync with node mapping service. The check only looks for local
    * keepers running on this node for which there is no registration in the nms. This method takes no action.
    * @return
    */
  private[this] def checkSync :Future[Boolean] =
    keyRegistry.list map { mappings =>
      val out = _keepers.filterNot { case ( key, reg ) =>
        mappings.exists( km => km == reg.mapping )
      }
      if ( out.nonEmpty ){
        log.error("{} found out of sync. Registrations {} do not exist in the key registry", context.self, out )
      }
      out.isEmpty
    }

  /**
   * based on a key and node passed in return the registered actor ref for the key
   * or create the ref for the key, add it to the map of keepers and return it. When creating
   * keeper refs, if the node represents the node this code is running on then the createKeeper
   * method will be called, otherwise if the node is a remote node then the getRemoteKeeperRef
   * method is called.
   */
  private[this] def doGetOrCreate( key :String, mp :KeyRegistration ) :ActorRegistration = getActor( key ) match {
    case Some( reg ) => reg
    case None =>
      var keeper :ActorRef = null
      if ( mp.address == me ) {
        keeper = createActor( key )
        context.watch( keeper )
      } else {
        keeper = getRemoteActorRef( key, mp.address )
      }

      val reg = ActorRegistration( mp, keeper )
      _keepers += ( key -> reg  )
      reg
  }

  /** send the keeper back to the sender if it is running, if not go through
    * the registration process and have the keeper created from the registration
    * process sent back to the sender.
    */
  private[this] def getOrRegister( key :String, action :ChannelAction, assign :Boolean, attempt :Int = 1 ) = getActor( key ) match {

    case Some( reg ) =>
      action.handle(reg)

    case None if _requestBuffer.contains(key) =>
      _requestBuffer = _requestBuffer.updated(key, (Platform.currentTime, action) +: _requestBuffer(key))

    case None =>
      val startedAt = Platform.currentTime
      _requestBuffer = _requestBuffer.updated(key , Nil)
       ( if ( assign ) fme else chooseAddress( key ) ).foreach { target =>
        if ( target != me ) {
          ( _registries(target) ask GetRegistration( key, true ) ) map {
            case reg :ActorRegistration =>
              RemoteActorRegistration( reg, action )
          } recover {
            case e :Throwable =>
              log.warning("Error [{}] calling getRegistration {} from target {} in DistributedActorRegistry.getOrRegister on attempt {}", e, key, target, attempt )
              NodeRegistrationError( key, action, attempt, e  )
          } pipeTo context.self
        } else {
          keyRegistry.setOrGet( key, target ) map { keyReg =>
            NodeRegistration( key, keyReg, action, startedAt )
          } recover {
            case e :Throwable =>
              log.warning("Error [{}] calling setOrGet( {}, {} ) in DistributedActorRegistry.getOrRegister on attempt", e, key, target, attempt )
              NodeRegistrationError( key, action, attempt, e ) // wire up retry semantics
          } pipeTo context.self
        }
       }
  }

  def getClusterRegistrations :Future[Map[Address,Map[String,ActorRegistration]]] = {
    Future.sequence {
      _registries.map { case ( address, aRef ) =>
        ( aRef ask GetRegistrations ).mapTo[Map[String,ActorRegistration]].map { mp => (address, mp) }
      }
    } map { reg =>
      reg.toMap + ( me -> _keepers.toMap )
    }
  }

  /**
   * return the keeper for the key if it exists. If it doesn't exist
   * None will be returned.
   *
   * @return Option[ActorRegistration]
   */
  def getActor( key :String ) = _keepers get key

  /* ######################################################
   * Just Command Handlers Below
   */

  /**
   * Main receive block for the registry.
   */
  final def receive :Receive = LoggingReceive {
    dropFilter        orElse
    clusterEvents     orElse
    keeperCommands    orElse
    handleRunning     orElse
    handleDeadLetters orElse
    doRec orElse unhandled }


  /** drop this messages */
  private[this] def dropFilter :Receive = {
    case DeadLetter( PoisonPill , _, _ ) => // drop
    //case DeadLetter( Terminate(), _, _ ) => // drop
  }

  /** handle cluster events. This will put together the hash ring and
    * will create remote actors for all remote keepers                */
  private[this] def clusterEvents :Receive = {

    case c: CurrentClusterState if c.leader.isDefined =>
      println(s"\n\n received new cluster state in AReg ${c} \n\n")
      _registries = c.allUp.filterNot(_ == me).map { address =>
        val reg = context.actorFor(RootActorPath(address) / context.self.path.elements )
        ( address -> reg )
      }.toMap
      val master = c.leader.get == me

      log.info("{} Actor Registry received cluster changed event: {}", context.self, c)

      val addys = c.allUp

      _keepers.foreach { case ( key, reg ) =>
        if ( ! addys.contains(reg.mapping.address) ) {
          _keepers.remove(key) foreach { kr =>
            if (master) {
              log.info("Master DistributedActorRegistry for component {} removing key {}", context.self, key)
              keyRegistry.remove(key, -1) onComplete {
                case util.Failure( t ) =>
                  log.warning("Error removing key {} from actor registry {}. Error: {}", key, context.self, t)
                case util.Success( true ) =>
                  log.debug("Removed key {} from actor registry {}", key, context.self )
                case util.Success( false ) =>
                  log.warning("Failed to remove key {} from actor registry {}", key, context.self )
              }
            }
          }
        }
      }

      if ( master ) {
        keyRegistry.list map { mappings =>
          mappings.map { mapping =>
            if ( ! addys.contains(mapping.address) ) {
              keyRegistry.remove(mapping.key, mapping.version) onComplete {
                case util.Failure( t ) =>
                  log.warning( "Error removing key {} from keeper registry {}. Error :{}", mapping.key, context.self, t)
                case util.Success( true ) =>
                  log.info("Removed key {} from keeper registry {}", mapping.key, context.self )
                case util.Success( false ) =>
                  log.warning("Failed to remove key {] from keeper registry {}", mapping.key, context.self )
              }
            }
          }
        }
      }

    case e :MemberEvent => // drop
      println(s"\n\n got memeber event ${e}\n\n")
      clusterExtension.sendCurrentClusterState(context.self)

    case e :ClusterDomainEvent => // shizzzz
      println(s"\n\n DOING NOTHING got cluster domain event ${e}\n\n")
      //clusterExtension.sendCurrentClusterState(context.self)

  }

  /** handle keeper specific commands */
  private[this] def keeperCommands :Receive = {

    case GetRegistrations =>
      sender ! keepers.toMap

    case GetClusterRegistrations =>
      getClusterRegistrations pipeTo context.sender

    case CheckSync =>
      val sender = context.sender
      val me     = context.self
      checkSync onSuccess {
        case true =>
          sender ! true
        case false =>
          sender ! false
          me ! new ActorRegistryOutOfSyncException
      }

    case GetRegistration( key, assign ) =>
      getOrRegister(key, Reply(context.sender), assign)

    case ex :ActorRegistryOutOfSyncException =>
      log.error(ex, "Received out of sync exception. Blowing up and rebuilding")
      throw ex

    case FlushRequestBuffer( false ) =>
      val now = Platform.currentTime
      val remaining = _requestBuffer.map { case ( key, seq ) =>
        val ( expired, buffer ) = seq.partition { case ( ts, action ) => now - ts >= requestBufferMax }
        expired.foreach { case ( ts, action ) =>
          context.self ! NodeRegistrationError( key, action, 1, new ActorRequestBufferTimeoutException("Key %s expired in request buffer after %d millis.".format(key, requestBufferMax)))
        }
        ( key, buffer )
      }

      _requestBuffer = remaining.filterNot(_._2.isEmpty) // filter out empty seqs

    case FlushRequestBuffer( true ) =>
      _requestBuffer.foreach { case ( key, seq ) =>
        seq.foreach { case ( ts, action ) =>
          context.self ! NodeRegistrationError( key, action, 1, new ActorRequestBufferTimeoutException("Key %s expired in request buffer after %d millis.".format(key, requestBufferMax)))
        }
      }

      _requestBuffer = Map.empty

  }

  /** handle key mappings. */
  private[this] def handleRunning :Receive = {
    case KeyRegistered( km @ KeyRegistration( key, address, version, _ ) ) =>
      if ( address != me) {
        log.debug("Remote KeyRegistered. Adding remote keeper ref for key {} on {}", key, address)
        doGetOrCreate( key, km )
      }

    case KeyDeregistered( key, _ ) =>
      log.debug("Key {} deregistered. Removing ref.", key)
      _keepers.remove( key ) foreach {

        case kr if kr.mapping.address == me =>
          context.stop(kr.actor)
          _registries.values.foreach { _ ! ActorStopped( kr.mapping ) }

        case _ => // do nothing

      }

    case ActorStopped( km ) =>
      _keepers.remove(km.key) match {
        case Some( reg ) if km.version != reg.mapping.version =>
          _keepers + ( km.key -> reg )
          log.debug("received keeper stopped. Stopping {}" format( km ))

        case _ => // do nothing
          log.debug("received keeper stopped but no local keeper to stop")
      }

    case ActorStarted( reg ) =>
      getActor( reg.mapping.key ) match {
        case Some( existing ) if existing.mapping.version < reg.mapping.version => // update
          _keepers += ( reg.mapping.key -> reg )
        case None =>  // set
          _keepers += ( reg.mapping.key -> reg )

        case _ => // ignore
      }

    case RemoteActorRegistration( reg, action ) =>
      getActor( reg.mapping.key ) match {

        case Some( existing ) if existing.mapping.version < reg.mapping.version => // update
          _keepers += ( reg.mapping.key -> reg )
          action.handle(reg)
        case None =>  // set
          _keepers += ( reg.mapping.key -> reg )
          action.handle(reg)

        case _ => // ignore
      }

    case NodeRegistration( key, mapping, action, startedAt ) =>
      val keeper = doGetOrCreate( key , mapping )
      action.handle( keeper )

      val buffered = _requestBuffer.get(key)
      _requestBuffer = _requestBuffer - key
      buffered.map { seq =>
        seq.reverse.foreach { case ( _, bufferedAction ) => bufferedAction.handle( keeper ) }
      }

      val duration = Platform.currentTime - startedAt
      if ( me == mapping.address ) {
        //Diva.stats.add(localReg, duration.toInt )
        log.debug("local node registered for key {} => node {} in {} millis", key, mapping, duration)
      } else {
        //Diva.stats.add(remoteReg, duration.toInt )
        log.debug("remote node registered for key {} => node {} in {} millis", key, mapping, duration)
      }

    case msg if mClazz.exists( _.isAssignableFrom( msg.getClass ) ) && _activeKeyResolver.isDefinedAt( msg.asInstanceOf[M] ) =>
      val key = _activeKeyResolver( msg.asInstanceOf[M] )
      getOrRegister( key, Forward( msg, context.sender ), true ) // false ?

    case Terminated( aRef ) =>
      val key = pathToKey( aRef.path )

      _keepers.remove( key ) match {

        case Some( reg ) if ( ! reg.actor.isTerminated ) =>
          context.stop( reg.actor )
          log.debug("Stopping non-terminated keeper {} for key {} in {}", reg.actor,  key, context.self )

        case Some( removed ) =>
          log.debug("Removing terminated keeper for key {} from registry {}", key, context.self )
          keyRegistry.remove( key, removed.mapping.version ) onFailure {
            case e => log.error(e, "Exception removing key {} from keyRegistryService", key )
          }

        case None =>
          log.debug("Received terminated [{}] message but no keeper found.",aRef)

      }

    case e @ NodeRegistrationError(key, action, attempt, error ) =>
      if ( regRetryMax <= attempt ) {
        action.error(new ActorRegistryException(s"Could not register key ${key} in actor registry ${context.self}", error))
        val me = context.self
        checkSync map {
          case true =>
            log.info("Ran checkSync after node registration error in {} keeper for key {}. Success. No action taken.",context.self, key)
          case false =>
            log.error("Ran checkSync after node registration error in {} keeper for key {}. No Success. Blowing up.",context.self, key)
            me ! ActorRegistryOutOfSyncException
        } recover {
          case e =>
            log.error(e, "CheckSync failed after node registration error in {} keeper for key {}. Blowing up.", context.self, key)
            me ! ActorRegistryOutOfSyncException
        }
      } else {
        log.warning("NodeRegistrationError {}. Retrying getOrRegister, attempt {}", e, attempt + 1 )
        getOrRegister(key, action, false, attempt + 1 )
      }


    case Failure( t ) =>
      log.error(t, "Major unknown problem in KeeperRegistry {}. Blowing up and restarting", context.self.path )
      throw new ActorRegistryException(context.self.path.toString, t)
  }

  private[this] def handleDeadLetters :Receive = {
    // need to check recipient to prevent message loops
    case dead :DeadLetter if mClazz.exists( _.isAssignableFrom( dead.message.getClass) ) && _activeKeyResolver.isDefinedAt( dead.message.asInstanceOf[M] ) =>
      if ( dead.recipient.path.name == "deadLetters" || dead.sender == null ){
        log.debug("Dropping dead letter due to null sender or deadLetter recipient. msg {}", dead)
      } else {

        val key = _activeKeyResolver( dead.message.asInstanceOf[M] )

        getActor( key ) match {

          case Some( reg ) =>
            log.debug("Found keeper registration {} from dead letter msg {} and returning to sender", reg, dead )
            reg.actor.tell( dead.message , dead.sender )

          case None =>
            getOrRegister( key, Forward(dead.message, dead.sender), true ) // false?

        }
      }

    case dead :DeadLetter =>
      if ( mClazz.exists( _.isAssignableFrom( dead.message.getClass) ) ) {
        log.debug("KeeperRegistry {} dropping recycled DeadLetter. No resolvable key. MSG: {}", context.self.path, dead.toString.cap(200))
      }
  }

  /** this can be overridden if needed */
  def unhandled :Receive = {
    case msg =>
      log.error("KeeperRegistry {} recieved unandled msg {} from sender {}", context.self.path, msg.toString.cap(200), context.sender )
  }

}

trait DistributionStrategy {  self :DistributedActorRegistry[_] =>

  def chooseAddress( key :String ) :Future[Address]

}

trait PickSelfStrategy extends DistributionStrategy { self :DistributedActorRegistry[_] =>

  val addy = Future.successful( me )

  def chooseAddress( key :String ) = addy

}

trait ConsistentHashingStrategy extends DistributionStrategy { self :DistributedActorRegistry[_] =>

  implicit val to = Timeout( context.system.settings.config.getInt("pakkxos.defaults.timeout_sec") seconds )

  def chooseAddress( key :String ) = ( actor ask Choose(key) ).mapTo[Address]

  case class Choose(key :String)
  class StrategyActor extends Actor {

    var ring = HashRing.empty[Address].add(me)

    def receive = {

      case c :CurrentClusterState =>
        val addys = c.members.filter(_.status == MemberStatus.Up).map(_.address)
        ring = HashRing.evenlySpaced( c.leader.map( addys + ).getOrElse(addys) )

      case c :ClusterDomainEvent =>
        clusterExtension.sendCurrentClusterState(context.self)

      case Choose( key ) =>
        context.sender ! ring.node(key)

    }
  }

  val actor = context.actorOf(Props[StrategyActor])

  clusterExtension.subscribe(actor, classOf[ClusterDomainEvent])

}



