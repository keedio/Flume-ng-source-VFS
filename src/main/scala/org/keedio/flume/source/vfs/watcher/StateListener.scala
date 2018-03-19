package org.keedio.flume.source.vfs.watcher

/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@keedio.com
  * Keedio
  */
/**
  * Mix-in this trait to become a listener, implementing
  * what to do when an event ins received.
  */
trait StateListener {
  def statusReceived(stateEvent: StateEvent): Unit
}