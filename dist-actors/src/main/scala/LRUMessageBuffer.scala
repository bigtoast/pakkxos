package com.github.bigtoast.pakkxos

import akka.actor.{ActorRef, Actor}
import collection.immutable.Queue

/**
 * This trait is intended to be used by state machines when we want to buffer messages intended
 * to be processed in a different state.
 *
 * By default this will buffer up to 1000 messages. When bufferSize is reached, the oldest messages
 * will start being dropped.
 *
 */
trait LRUMessageBuffer { self :Actor =>

  type Message = (Any, ActorRef)

  lazy val _bufferSize = context.system.settings.config.getInt("pakkxos.lru_buffer_size")

  private val _buffer = new collection.mutable.Queue[Message]()

  /**
   * Buffer a message. If buffer size is reached, old messages will be dropped
   * as new ones are added.
   */
  def buffer( msg :Any, sender :ActorRef ) = {
    if ( _buffer.size >= _bufferSize ) {
      _buffer.dequeue()
    }

    _buffer.enqueue( ( msg, sender ) )
  }

  /**
   * dequeue a message. If the buffer is empty this will throw a
   * NoSuchElementException
   */
  def dequeue :Message = _buffer.dequeue()

  def size = _buffer.size

  /**
   * clear the buffer returning what was in it
   */
  def clear :Seq[Message] = { _buffer.dequeueAll( _ => true ) }

  /**
   * Flush the buffer. This will send all messages in the buffer
   * to the actor mixed in with this trait.
   */
  def flush() {
    while( _buffer.nonEmpty ){
      val ( msg, channel ) = _buffer.dequeue()
      context.self.tell( msg, channel )
    }
  }

}
