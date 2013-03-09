package com.github.bigtoast

import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.{Member, MemberStatus}
import java.util.UUID._
import scala.Some
import akka.cluster.ClusterEvent.CurrentClusterState

package object pakkxos {

  class RichCurrentClusterView( cluster :CurrentClusterState ) {
    lazy val up = cluster.members.collect { case m :Member if m.status == MemberStatus.Up => m.address }

    def hasLeader = cluster.leader.isDefined

    lazy val allUp = cluster.leader match {
      case Some( leader ) =>
        up + leader
      case None =>
        up
    }
  }

  implicit def toRichCluster( cluster :CurrentClusterState ) = new RichCurrentClusterView(cluster)

  class StringHelper( s :String ) {
    def uuid = fromString(s)

    /** cap this string at a given length */
    def cap( length :Int ) = {
      if ( s.length > length )
        s.substring(0,length)
      else
        s
    }

    /** pad the left side of the string with char. If the string is greater in length
      * than length, the original string will be returned
      */
    def padL(char :Char, length :Int ) = s.length > length match {
      case true  => s
      case false =>
        val str = new StringBuilder
        for ( i <- 1 to (length - s.length ) )
          str.append(char)
        str.append(s).toString()
    }

    def padR(char :Char, length :Int ) = s.length > length match {
      case true  => s
      case false =>
        val str = new StringBuilder(s)
        for ( i <- 1 to (length - s.length ) )
          str.append(char)
        str.toString()
    }
  }

  implicit def toStringHelper( s :String) = new StringHelper(s)

}