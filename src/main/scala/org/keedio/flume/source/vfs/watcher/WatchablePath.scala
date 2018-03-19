package org.keedio.flume.source.vfs.watcher

import java.util.concurrent._

import org.apache.commons.vfs2._
import org.apache.commons.vfs2.impl.DefaultFileMonitor

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * Created by luislazaro on 4/9/15.
  * lalazaro@keedio.com
  * Keedio
  */

class WatchablePath(uri: String, refresh: Int, start: Int, regex: Regex, fileObject: FileObject, listener: StateListener) {

  //list of susbcribers(observers) for changes in fileObject
  private val listeners: ListBuffer[StateListener] = new ListBuffer[StateListener]
  private val children: Array[FileObject] = fileObject.getChildren
  addEventListener(listener)

  //observer for changes to a file
  private val fileListener = new VfsFileListener {
    override def fileDeleted(fileChangeEvent: FileChangeEvent): Unit = {
      val eventDelete: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_DELETE)
      if (isValidFilenameAgainstRegex(eventDelete)) {
        fireEvent(eventDelete)
        //fileObject.refresh()
        }
    }

    override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = {
      val eventChanged: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_MODIFY)
      if (isValidFilenameAgainstRegex(eventChanged)) {
        fireEvent(eventChanged)
        //fileObject.refresh()
      }
    }

    override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = {
      val eventCreate: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_CREATE)
      if (isValidFilenameAgainstRegex(eventCreate)) {
        fireEvent(eventCreate)
        //fileObject.refresh()
      }
    }

    override def fileDiscovered(fileChangeEvent: FileChangeEvent): Unit = {
      val eventDiscovered: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_DISCOVER)
      if (isValidFilenameAgainstRegex(eventDiscovered)) {
        fireEvent(eventDiscovered)
        //fileObject.refresh()
      }
    }
  }

  //Thread based polling file system monitor with a 1 second delay.
  private val defaultMonitor: DefaultFileMonitor = new DefaultFileMonitor(fileListener)
  defaultMonitor.setDelay(secondsToMiliseconds(refresh))
  defaultMonitor.setRecursive(true)
  defaultMonitor.addFile(fileObject)
  children.foreach( child => fileListener.fileDiscovered(new FileChangeEvent(child)))

  // the number of threads to keep in the pool, even if they are idle
  private val corePoolSize = 1
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize)
  //Creates and executes a one-shot action that becomes enabled after the given delay
  private val tasks: ScheduledFuture[_] = scheduler.schedule(
    getTaskToSchedule(),
    start,
    TimeUnit.SECONDS
  )

  /**
    * Call this method whenever you want to notify the event listeners about a
    * FileChangeEvent.
    * Filtering monitored files via regex is made after an event is fired.
    */
  def fireEvent(stateEvent: StateEvent): Unit = {
    listeners foreach (_.statusReceived(stateEvent))
  }

  /**
    * Check filename string against regex
    *
    * @param stateEvent
    * @return
    */
  def isValidFilenameAgainstRegex(stateEvent: StateEvent) = {
    val fileName: String = stateEvent.getFileChangeEvent.getFile.getName.getBaseName
    regex.findFirstIn(fileName).isDefined
  }

  /**
    * Add element to list of registered listeners
    *
    * @param listener
    */
  def addEventListener(listener: StateListener): Unit = {
    listener +=: listeners
  }

  /**
    * Remove element from list of registered listeners
    *
    * @param listener
    */
  def removeEventListener(listener: StateListener): Unit = {
    listeners.find(_ == listener) match {
      case Some(listener) => {
        listeners.remove(listeners.indexOf(listener))
      }
      case None => ()
    }
  }

  /**
    *
    * auxiliar for using seconds where miliseconds is requiered
    *
    * @param seconds
    * @return
    */
  implicit def secondsToMiliseconds(seconds: Int): Long = {
    seconds * 1000
  }

  /**
    * Make a method runnable and schedule for one-shot
    *
    * @return
    */
  def getTaskToSchedule(): Runnable = {
    new Runnable {
      override def run(): Unit = {
        defaultMonitor.start()
      }
    }
  }


}