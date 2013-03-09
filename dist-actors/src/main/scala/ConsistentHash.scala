package com.github.bigtoast.pakkxos

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

object HashRing {

  private[this] val md = java.security.MessageDigest.getInstance("SHA-1")

  def sha1[T]( obj :T ) :Array[Byte] =
    if ( obj.isInstanceOf[String] ) {
      md.digest(obj.asInstanceOf[String].getBytes)
    } else {
      var ary :Array[Byte] = null
      var os :ObjectOutputStream = null
      try {
        val ba = new ByteArrayOutputStream
        os = new ObjectOutputStream( ba )
        os.writeObject(obj)
        ary = ba.toByteArray()
      } finally {
        os.close
      }
      md.digest( ary )
    }

  def sha1Long[K]( obj :K ) :Long = {
    val bytes = sha1(obj)
    val hash =
      ((( bytes(7) & 0xFF ).toLong << 56 ) |
       (( bytes(6) & 0xFF ).toLong << 48 ) |
       (( bytes(5) & 0xFF ).toLong << 40 ) |
       (( bytes(4) & 0xFF ).toLong << 32 ) |
       (( bytes(3) & 0xFF ).toLong << 24 ) |
       (( bytes(2) & 0xFF ).toLong << 16 ) |
       (( bytes(1) & 0xFF ).toLong << 8 )  |
       (( bytes(0) & 0xFF ).toLong ) )
    hash
  }

  def empty[K] = EmptyRing[K]

  def build[K]( it :Iterable[K] ) =
    it.foldLeft[HashRing[K]](empty[K]){ case (ring, value) => ring.add(value) }

  def evenlySpaced[K]( it :Iterable[K] ) =
    it.zipWithIndex.foldLeft[HashRing[K]]( EmptyRing[K] ){
      case ( ring, ( key, idx ) ) =>
        ring.add(key, Long.MinValue + ((Long.MaxValue / it.size) * 2 ) * (idx + 1) ) }

}

trait Node[K] {
  def isEnd :Boolean
  def isLast :Boolean
}

case class End[K]() extends Node[K] {
  val isEnd = true
  val isLast = false
}

case class Cons[K]( value :K, position :Long, next :Node[K] ) extends Node[K] {
  def isLast = next.isEnd
  val isEnd = false
}

/**
 * A hash ring that fits my use case. you can add values to the ring which will
 * be added based on a hash or values can be added in a position in the ring.
 */
trait HashRing[K] {
  import HashRing._

  def ring :Node[K]

  def head :K = ring match {
    case Cons( value, _, _ ) => value
    case _ => throw new IndexOutOfBoundsException
  }

  def isEmpty :Boolean

  def node( key :String ) :K = {
    val pos = sha1Long(key)
    def step( current :Node[K] ) :K = current match {
      case End() => throw new IndexOutOfBoundsException
      case Cons( value, position, _) if pos < position => value
      case Cons( value, position, End() ) => head
      case Cons( _, _, next )  => step( next )
    }
    step( ring )
  }

  def add( value :K ) :HashRing[K] = add(value, sha1Long(value))

  def add( value :K, key :String ) :HashRing[K] = add( value, sha1Long(key) )

  def add( value :K , position :Long ) :HashRing[K] = {
    // rewrite tail recursive
    def add(hd :Node[K], current :Node[K] ) :Node[K] = current match {
      case e @ End() =>
        join( hd, Cons( value, position, e) )

      case c @ Cons( v, p, next @ Cons(_,_,_) ) if ( position < next.position ) =>
        join( hd, Cons(v, p, Cons( value, position, next) ) )

      case c @ Cons( v, p, next @ End() ) =>
        join( hd, Cons(v, p, Cons( value, position, next ) ) )

      case c @ Cons(_,_,next) =>
        join( hd, add(c, next) )
    }

    def join( head :Node[K], tail :Node[K]) = head match {
      case Cons( value, position, _ ) => Cons( value, position, tail )
      case End()  => tail
    }

    add( End[K], ring ) match {
      case c @ Cons(_,_,_) => FullRing( c )
      case End()  => EmptyRing()
    }
  }
}

case class EmptyRing[K]() extends HashRing[K] {
  val isEmpty = true
  val ring = End[K]()
}

case class FullRing[K]( ring :Cons[K] ) extends HashRing[K] {
  val isEmpty = false
}
