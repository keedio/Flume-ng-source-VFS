package org.keedio.flume.source.vfs.watcher

/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@keedio.com
  * Keedio
  */
/**
  * Available file's states.
  */
class State(name: String) {
  override def toString(): String = {
    name
  }
}

object State extends Serializable {
  final val ENTRY_CREATE: State = new State("entry_create")
  final val ENTRY_DELETE: State = new State("entry_delete")
  final val ENTRY_MODIFY: State = new State("entry_modify")
  final val ENTRY_DISCOVER: State = new State("entry_discover")
}
