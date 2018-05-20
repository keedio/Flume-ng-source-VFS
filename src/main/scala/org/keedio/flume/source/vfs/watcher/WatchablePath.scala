package org.keedio.flume.source.vfs.watcher

import java.util.Date
import java.util.concurrent._

import org.apache.commons.vfs2._
import org.apache.commons.vfs2.impl.DefaultFileMonitor
import org.keedio.flume.source.vfs.config.SourceHelper
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * Created by luislazaro on 4/9/15.
  * lalazaro@keedio.com
  * Keedio
  */

class WatchablePath( fileObject: FileObject, listener: StateListener,
                    sourceName: String, sourceHelper: SourceHelper) {

  val LOG: Logger = LoggerFactory.getLogger(classOf[WatchablePath])

  private val includePattern: Regex = sourceHelper.getPatternFilesMatch
  private val processDiscovered = sourceHelper.isProcessFilesDiscovered
  private val timeOut: Integer = sourceHelper.getTimeoutProcessFiles
  private val recursiveSearch = sourceHelper.isRecursiveSearchDirectory
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
        fs.getFileSystemManager.getFilesCache.removeFile(fs, fileChangeEvent.getFile.getName)
      }
    }

    override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = {
      val eventChanged: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_MODIFY)
      if (isValidFilenameAgainstRegex(eventChanged) && isEventValidStatus(eventChanged)) {
        fireEvent(eventChanged)
      }
    }

    override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = {
      val eventCreate: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_CREATE)
      if (isValidFilenameAgainstRegex(eventCreate) && isEventValidStatus(eventCreate)) {
        fireEvent(eventCreate)
      }
    }

    override def fileDiscovered(fileChangeEvent: FileChangeEvent): Unit = {
      val eventDiscovered: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_DISCOVER)
      if (isValidFilenameAgainstRegex(eventDiscovered) && isEventValidStatus(eventDiscovered)) {
        fireEvent(eventDiscovered)
      }
    }
  }

  //Thread based polling file system monitor with a refresh second delay.
  private val defaultMonitor: DefaultFileMonitor = new DefaultFileMonitor(fileListener)
  defaultMonitor.setDelay(secondsToMiliseconds(sourceHelper.getDelayBetweenRuns))
  defaultMonitor.setChecksPerRun(sourceHelper.getMaxFilesCheckPerRun)

  ///defaultMonitor.setRecursive(recursiveSearch) --> has no effect ( JIRA-VFS-569)
  if (sourceHelper.isRecursiveSearchDirectory) {
    defaultMonitor.addFile(fileObject)
    addSufolderOnStart(fileObject.getChildren.toList)
  } else {
    val folders: List[FileObject] = children.toList.filter(_.isFolder)
    defaultMonitor.addFile(fileObject)
    folders.foreach(folder => defaultMonitor.removeFile(folder))
  }

  processDiscovered match {
    case true =>
      if (sourceHelper.isRecursiveSearchDirectory) {
        processDiscover(children.toList)
      } else {
        processDiscover(children.toList.filter(_.isFile))
      }
      LOG.info("Source " + sourceName + " has property 'process.discovered.files' set to " + processDiscovered +
        ",process files that exists before start source.")
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
    2,
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
    includePattern.findFirstIn(fileName).isDefined
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
    val file: FileObject = event.getFileChangeEvent.getFile

    if (!file.exists()) {
      LOG.warn("Event " + event.getState + " was triggered with file name " + event.getFileChangeEvent.getFile.getName
        .getBaseName + " but file not exists.")
      status = false
    } else if (!file.isReadable) {
      LOG.warn(event.getFileChangeEvent.getFile.getPublicURIString + " is not readable.")
      status = false
    } else if (!file.isFile) {
      LOG.warn(event.getFileChangeEvent.getFile.getName.getBaseName + " is not a regular file.")
      status = false
    } else  {

      val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
      val lastModifiedTime = file.getContent.getLastModifiedTime
      val lastModifiedTimeAccuracy = file.getFileSystem.getLastModTimeAccuracy

      timeOut.toInt match {
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

  /**
    * Iterate over element of subfolders
    *
    * @param children
    */
  def processDiscover(children: List[FileObject]) {
    children.foreach(child => {
      if (child.isFile) {
        fileListener.fileDiscovered(new FileChangeEvent(child))
      } else if (child.isFolder) {
        processDiscover(child.getChildren.toList)
      }
    }
    )
  }

  /**
    * iterate over folders and add them to monitor.
    *
    * @param children
    */
  def addSufolderOnStart(children: List[FileObject]) {
    children.foreach(child => {
      if (child.isFolder) {
        defaultMonitor.addFile(child)
        addSufolderOnStart(child.getChildren.toList)
      }
    }
    )
  }

}