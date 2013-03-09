package com.github.bigtoast.pakkxos

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import akka.event.Logging
import concurrent.{Future, Promise}
import concurrent.duration._
import PaxosDistributedKeyManager.Request._
import PaxosDistributedKeyManager._
import ProtocolData.KeyExists
import compat.Platform

class PaxosDistributedKeyRegistry(
    val context :String,
    aRef        :ActorRef,
    system      :ActorSystem ) extends DistributedKeyRegistry {

  implicit val ctx = system.dispatcher

  val log = Logging(system, this.getClass)

  //def ping = Up("paxosDKR")

  def toKey(path :String) = path.stripPrefix(context + "/")

  def toContext(path :String) = path.split("/")(0)

  def fromKey(key :String) = context + "/" + key

  val serviceContext = context

  implicit val to = Timeout( system.settings.config.getInt("pakkxos.defaults.timeout_sec") seconds )

  /**
   * Attempt to set the node associated with the key. If a node is already
   * registered with the key it will be returned instead and the registration
   * will fail. If the set is successful None will be returned.
   *
   * @return Some(Address) if registration exists | None if passed in node was set
   */
  def getOrSet(key: String, address: Address) :Future[KeyRegistration] =
    get(key) flatMap {
      case Some( km ) => Future.successful(km)
      case None       => setOrGet(key, address)
    }

  /**
   * Works just like getOrSet except that this implementation will try to set before
   * it tries to get.
   *
   */
  def setOrGet(key: String, address: Address) :Future[KeyRegistration] =
    set( key, address ) recoverWith {
      case e :ExistingKeyException =>
        get(key) flatMap {
          case Some( km ) => Future.successful(km)
          case None       => setOrGet(key, address)
        }
    }

  /**
   * Remove the registration associated with the key matching the set version.
   * If the version passed in is -1 then there will be a force removal
   *
   * @return true if removal was successful.
   */
  def remove(key: String, version: Long) =
    ( aRef ask Delete( fromKey(key), version ) ).mapTo[DeletedResponse].map {
      resp =>
        log.debug("Delete key {} succeded with resp {} in DNM service", key, resp)
        true
    } recover { case e =>
      log.warning("Delete key {} encountered error {}", key, e)
      false
    }

  /**
   * Return the Address associated with the given key. If a node is not registered with
   * the key then None will be returned.
   *
   * @return Some[Address] if node exists | None if node doesn't exist
   */
  def get(key: String) :Future[Option[KeyRegistration]] =
    ( aRef ask Get( fromKey(key) ) ).mapTo[DataResponse].map { data =>
      Some( KeyRegistration( key, data.value, data.stat.mzxid, data.stat.mTime ) )
    } recover {
      case er :NoSuchKey =>
        None
    }

  /**
   * Attempt to register the node with the key passed in. If the key is already registered
   * then an ExistingKeyException will be returned.
   *
   * @throws ExistingKeyException if the key is already registered.
   */
  def set(key: String, node: Address) :Future[KeyRegistration] =
    ( aRef ask Create( fromKey(key), node ) ).mapTo[DataResponse].map { data =>
      KeyRegistration( key, data.value , data.stat.mzxid, data.stat.mTime )
    } recover {
      case er :KeyExists =>
        throw new ExistingKeyException(key)
    }

  /** list current mappings */
  def list :Future[Seq[KeyRegistration]] =
    ( aRef ask GetAll ).mapTo[Seq[DataResponse]].map { seq =>
      seq.map { data => KeyRegistration( toKey(data.key), data.value, data.stat.mzxid, data.stat.mTime ) }
    }

  /**
   * Clear all node mappings.
   */
  def clear = ( aRef ask GetAll ).mapTo[Seq[DataResponse]].map { seq =>
    seq.foreach { data => remove(data.key, -1) }
  }

  /**
   * Clear all registrations for the given Address.
   */
  def clear(node: Address) =
    ( aRef ask GetAll ).mapTo[Seq[DataResponse]].map { seq =>
      seq.foreach { data =>
        if (data.value == node ) remove( data.key, -1 )
      }
    }

  /**
   * Add a listener to receive AddressMappingEvents
   */
  def addListener(listener: ActorRef) {
    eventFilter ! AddListener( listener )
  }

  /**
   * remove a listener. If the listener doesn't exist nothing will happen.
   */
  def removeListener(listener: ActorRef) {
    eventFilter ! RemoveListener( listener )
  }

  /**
   * shutdown the service if it makes sense to do so.
   */
  def close() {
    aRef ! RemoveListener( eventFilter )
    system.stop( eventFilter )
  }

  def init() { /*
    Await.result( (aRef ask GetStatus).mapTo[ProtocolRole], to.duration ) match {
      case ProtocolRole.ACCEPTOR_RUNNING | ProtocolRole.PROPOSER_RUNNING =>
        // success
      case role =>
        log.debug("SER service uninitialised ( {} ). Retrying init in 100 millis", role)
        Thread.sleep(500)
        init()
    }            */
  }

  lazy val eventFilter = system.actorOf(Props( new AddressMappingEventFilter ))

  class AddressMappingEventFilter extends Actor {

    var listeners :Set[ActorRef] = Set.empty

    def receive = {
      case AddListener( aRef ) =>
        listeners = listeners + aRef

      case RemoveListener( aRef ) =>
        listeners = listeners - aRef

      case data :DataResponse => // @todo strip out context
        if ( toContext( data.key) == serviceContext ) {
          val m = KeyRegistered( KeyRegistration( toKey(data.key), data.value , data.stat.mzxid, data.stat.mTime ) )
          listeners.foreach { _ ! m }
        }

      case data :DeletedResponse =>
        if ( toContext( data.key ) == serviceContext ) {
          val m = KeyDeregistered( toKey(data.key), Platform.currentTime )
          listeners.foreach { _ ! m }
        }

    }
  }

  aRef ! AddListener( eventFilter )

  init()

}
