package org.keedio.flume.source.vfs.watcher

import org.apache.commons.vfs2.FileChangeEvent

/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@keedio.com
  * Keedio
  */
class StateEvent(fileChangeEvent: FileChangeEvent, state: State) {
  def getState = state
  def getFileChangeEvent = fileChangeEvent
}
