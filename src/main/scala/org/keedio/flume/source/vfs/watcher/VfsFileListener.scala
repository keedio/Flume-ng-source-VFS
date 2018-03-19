package org.keedio.flume.source.vfs.watcher

import org.apache.commons.vfs2.{FileChangeEvent, FileListener}

/**
  * Created by luislazaro on 15/3/18.
  * lalazaro@keedio.com
  * Keedio
  */

/**
  * VfsFileListener mixes Filelistener and adds
  * a signature for dealing with files that neither
  * are been created, modified nor deleted, i.e. discovered
  */
trait VfsFileListener extends FileListener{
  def fileDiscovered(event: FileChangeEvent): Unit
}
