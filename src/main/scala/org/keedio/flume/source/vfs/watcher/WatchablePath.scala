package org.keedio.flume.source.vfs.watcher

import java.util.Date
import java.util.concurrent._

import org.apache.commons.vfs2._
import org.apache.commons.vfs2.impl.DefaultFileMonitor
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * Created by luislazaro on 4/9/15.
  * lalazaro@keedio.com
  * Keedio
  */

class WatchablePath(uri: String, refresh: Int, start: Int, regex: Regex, fileObject: FileObject, listener:
StateListener, processDiscovered: Boolean, sourceName: String, timeOut: Int) {

  val LOG: Logger = LoggerFactory.getLogger(classOf[WatchablePath])

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
        val fs = fileObject.getFileSystem
        fs.removeListener(fileChangeEvent.getFile, this)
      }
    }

    override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = {
      val eventChanged: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_MODIFY)
      if ((isValidFilenameAgainstRegex(eventChanged)) && (isEventValidStatus(eventChanged))) {
        fireEvent(eventChanged)
      }
    }

    override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = {
      val eventCreate: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_CREATE)
      if ((isValidFilenameAgainstRegex(eventCreate)) && (isEventValidStatus(eventCreate))) {
        fireEvent(eventCreate)
      }
    }

    override def fileDiscovered(fileChangeEvent: FileChangeEvent): Unit = {
      val eventDiscovered: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_DISCOVER)
      if ((isValidFilenameAgainstRegex(eventDiscovered)) && (isEventValidStatus(eventDiscovered))) {
        fireEvent(eventDiscovered)
      }
    }
  }

  //Thread based polling file system monitor with a 1 second delay.
  private val defaultMonitor: DefaultFileMonitor = new DefaultFileMonitor(fileListener)
  defaultMonitor.setDelay(secondsToMiliseconds(refresh))
  defaultMonitor.setRecursive(true)
  defaultMonitor.addFile(fileObject)

  processDiscovered match {
    case true =>
      children.foreach(child => fileListener.fileDiscovered(new FileChangeEvent(child)))
      LOG
        .info("Source " + sourceName + " has property 'process.discovered.files' set to " + processDiscovered + ", " +
          "process files that exists " +
          "before start source.")
    case false =>
      LOG
        .info("Source " + sourceName + " has property 'process.discovered.files' set to " + processDiscovered + ", do" +
          " " +
          "not process files that exists " +
          "before start source.")
  }

  // the number of threads to keep in the pool, even if they are idle
  private val corePoolSize = 1
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize)
  //Creates and executes a one-shot action that becomes enabled after the given delay
  private val tasks: ScheduledFuture[_] = scheduler.schedule(
    getTaskToSchedule(),
    timeOut,
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

  /**
    * Condition for an stateEvent to be invalid
    *
    * @param event
    * @return
    */
  def isEventValidStatus(event: StateEvent): Boolean = {
    var status = true
    if (!event.getFileChangeEvent.getFile.exists()) {
      LOG.error("File for event " + event.getState + " not exists.")
      status = false
    }

    if (!event.getFileChangeEvent.getFile.isReadable) {
      LOG.error(event.getFileChangeEvent.getFile.getPublicURIString + " is not readable.")
      status = false
    }

    if (!event.getFileChangeEvent.getFile.isFile) {
      LOG.error(event.getFileChangeEvent.getFile.getName.getBaseName + " is not a regular file.")
      status = false
    }

    val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
    val file: FileObject = event.getFileChangeEvent.getFile
    val lastModifiedTime = file.getContent.getLastModifiedTime
    val lastModifiedTimeAccuracy = file.getFileSystem.getLastModTimeAccuracy

    timeOut match {
      case 0 => status = true
      case _ =>
        val accurateTimeout = adjustTimeout(lastModifiedTimeAccuracy, timeOut)

        if (lastModifiedTimeExceededTimeout(lastModifiedTime, accurateTimeout)) {
          LOG
            .info("File " + fileName + " could be still being written or timeout may be too high, do not process yet." +
              " File will be checked in " + accurateTimeout + " seconds.")
          val schedulerDelay: ScheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize)
          //Creates and executes a one-shot action that becomes enabled after the given delay
          schedulerDelay.schedule(
            new Runnable {
              override def run(): Unit = {
                val updateModified = fileObject.resolveFile(fileName).getContent.getLastModifiedTime
                if (lastModifiedTimeExceededTimeout(updateModified, accurateTimeout)) {
                  status = false
                } else {
                  LOG.info("File " + fileName + " reached threshold last modified time, send to process.")
                  fileListener.fileCreated(new FileChangeEvent(file))
                }
              }
            },
            accurateTimeout,
            TimeUnit.SECONDS
          )
          status = false
        }
    }
    status
  }

  import java.util.Calendar

  /**
    * Determine whether the attribute 'lastModifiedTime' exceeded argument threshold(timeout).
    * If 'timeout' seconds have passed since the last modification of the file, file can be discovered
    * and processed.
    * If Datemodified is before than DateTimeout we can process, return true
    *
    * @param lastModifiedTime
    * @param timeout ,         configurable by user via property processInUseTimeout (seconds)
    * @return
    */
  def lastModifiedTimeExceededTimeout(lastModifiedTime: Long, timeout: Int): Boolean = {
    val dateModified = new Date(lastModifiedTime)
    val cal = Calendar.getInstance
    cal.setTime(new Date)
    cal.add(Calendar.SECOND, -timeout)
    val timeoutAgo = cal.getTime
    dateModified.compareTo(timeoutAgo) > 0
  }

  /**
    * Returns the timeout set by user but adjusted with the accuracy of the last modification time provided
    * by the file system.
    *
    * @param lastModifiedTimeAccuracy
    * @param baseTimeOut
    * @return
    */
  def adjustTimeout(lastModifiedTimeAccuracy: java.lang.Double, baseTimeOut: Int): Int = {
    val adjustedTimeout = lastModifiedTimeAccuracy.toInt match {
      case 0 => baseTimeOut
      case x if (x > 0) =>
        (baseTimeOut.toLong - x.toLong).toInt
      case _ => baseTimeOut
    }
    if (LOG.isDebugEnabled) {
      LOG.debug("The accuracy of the last modification time provided by file system is " + lastModifiedTimeAccuracy +
        " ms " + ", computed timeout is " + adjustedTimeout + " seconds")
    }

    adjustedTimeout
  }

}