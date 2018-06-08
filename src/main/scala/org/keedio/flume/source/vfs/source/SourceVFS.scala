package org.keedio.flume.source.vfs.source

import java.io._
import java.nio.charset.Charset
import java.nio.file._
import java.util
import java.util.concurrent.{Executors, _}

import org.apache.commons.vfs2.FileObject
import org.apache.flume.conf.Configurable
import org.apache.flume.event.SimpleEvent
import org.apache.flume.source.AbstractSource
import org.apache.flume.{ChannelException, Context, Event, EventDrivenSource}
import org.keedio.flume.source.vfs.config.{PropertiesHelper, SourceProperties}
import org.keedio.flume.source.vfs.metrics.SourceCounterVfs
import org.keedio.flume.source.vfs.util.SourceHelper
import org.keedio.flume.source.vfs.watcher._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@keedio.com
  * Keedio
  */
class SourceVFS extends AbstractSource with Configurable with EventDrivenSource {

  val LOG: Logger = LoggerFactory.getLogger(classOf[SourceVFS])
  private var mapOfFiles = mutable.HashMap[String, (Long, Long, Long)]()
  private val mapFileAvailability = new scala.collection.mutable.HashMap[FileObject, Boolean]() with scala.collection
  .mutable.SynchronizedMap[FileObject, Boolean]
  private var sourceVFScounter = new org.keedio.flume.source.vfs.metrics.SourceCounterVfs("")
  private val executor: ExecutorService = Executors.newFixedThreadPool(10)
  private var sourceName: String = ""
  private var propertiesHelper: PropertiesHelper = _
  private var watchablePath: WatchablePath = _
  private val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor

  val procFilesCollector = new Runnable() {
    override def run(): Unit = {
      if (!mapFileAvailability.isEmpty) {
        mapFileAvailability.keySet
          .foreach(file => postProcessFile(propertiesHelper.getActionToTakeAfterProcessingFiles, file))
      }
    }
  }

  val listener = new StateListener {
    override def statusReceived(event: StateEvent): Unit = {
      event.getState.toString() match {
        case "entry_create" => {
          val thread = new Thread() {
            override def run(): Unit = {
              val file: FileObject = event.getFileChangeEvent.getFile
              val fileName = file.getName.getBaseName
              val fileContent = file.getContent
              val fileSize = fileContent.getSize
              mapFileAvailability += (file -> false)
              if (mapOfFiles.contains(fileName)) {
                LOG
                  .info("File  " + fileName + " was already processed, do nothing !. If desired behavior is to " +
                    "reprocess "
                    + "file, set property " + SourceProperties
                    .RETAIN_DELETED_FILES_IN_MAP_WHEN_POSTPROCESS + " to false (or just remove property).")
                mapFileAvailability += (file -> true)
              } else {
                LOG.info("Source " + sourceName + " received event: " + event.getState
                  .toString() + " file " + fileName)
                LOG.info(Thread.currentThread().getName + " started processing new file: " + fileName)
                val linesRead = readStreamLines(file, 0L)
                if (linesRead != 0) {
                  mapFileAvailability += (file -> true)
                  LOG.info("End processing new file: " + fileName)
                  sourceVFScounter.incrementFilesCount()
                } else {
                  LOG.info("lines read " + linesRead + " do nothing")
                }
              }
            }
          }
          executor.execute(thread)
        }

        case "entry_modify" => {
          val thread = new Thread() {
            override def run(): Unit = {
              val file: FileObject = event.getFileChangeEvent.getFile
              val fileName = file.getName.getBaseName
              val fileContent = file.getContent
              val fileSize = fileContent.getSize
              val lastModifiedTime = file.getContent.getLastModifiedTime
              LOG.info("Source " + sourceName + " received event: " + event.getState
                .toString() + " file " + fileName)
              val filesValue = mapOfFiles.getOrElse(fileName, (0L, 0L, 0L))
              val prevLinesRead = filesValue._1
              val prevModifiedTime = filesValue._2
              val prevSize = filesValue._3
              val available = mapFileAvailability.getOrElse(file, true)
              available match {
                case true => {
                  if (prevSize == fileSize && lastModifiedTime == prevModifiedTime) {
                    LOG.info("File exists in map of files, previous size of file is " + prevSize + " " + Thread
                      .currentThread().getName + " nothing to do, file remains unchanged " + fileName)
                    Unit
                  } else if (prevSize == fileSize && lastModifiedTime != prevModifiedTime) {
                    LOG.info("File exists in map of files, size is the same but lastModifiedTime changed " + Thread
                      .currentThread().getName +
                      " started processing modified file: " + fileName)
                    mapFileAvailability += (file -> false)
                    val linesRead = readStreamLines(file, 0L)
                    if (linesRead != 0) {
                      LOG.info("End processing modified file: " + fileName)
                      mapFileAvailability += (file -> true)
                    }

                  } else {
                    val aux = fileSize > prevSize match {
                      case true => mapOfFiles.getOrElse(fileName, (0L, 0L, 0L))._1
                      case false => 0L
                    }
                    LOG.info("File exists in map of files, previous lines of file are " + prevLinesRead + " " + Thread
                      .currentThread().getName +
                      " started processing modified file: " + fileName)
                    mapFileAvailability += (file -> false)
                    val linesRead = readStreamLines(file, aux)
                    if (linesRead != 0) {
                      LOG.info("End processing modified file: " + fileName)
                      mapFileAvailability += (file -> true)
                    } else {
                      LOG.info("lines read " + linesRead + " do nothing")
                    }
                  }
                }
                case false => LOG.debug("Status is available " + available + " by " + event.getState.toString() + " " +
                  "for filename " + fileName)
              }
            }
          }
          executor.execute(thread)
        }

        case "entry_delete" => {
          val thread: Thread = new Thread() {
            override def run(): Unit = {
              val file: FileObject = event.getFileChangeEvent.getFile
              val fileName = file.getName.getBaseName
              if (!propertiesHelper.isKeepFilesInMap) {
                mapOfFiles -= fileName
              }
              LOG.info("Source " + sourceName + " received event: " + event.getState
                .toString() + " file " + fileName)
              mapFileAvailability -= file
            }
          }
          executor.execute(thread)
        }

        case "entry_discover" => {
          val thread = new Thread() {
            override def run(): Unit = {
              val file: FileObject = event.getFileChangeEvent.getFile
              val fileName = file.getName.getBaseName
              val lastModifiedTime = file.getContent.getLastModifiedTime
              val fileSize = file.getContent.getSize
              mapFileAvailability += (file -> false)
              LOG.info("Source " + sourceName + " received event: " + event.getState
                .toString() + " file " + fileName)
              mapOfFiles.get(fileName).isDefined match {
                case false =>
                  LOG.info(Thread.currentThread().getName + " started processing file discovered: " + fileName)
                  val linesRead = readStreamLines(file, 0L)
                  if (linesRead != 0) {
                    LOG.info("End processing discovered file: " + fileName)
                    mapFileAvailability += (file -> true)
                    sourceVFScounter.incrementFilesCount()
                  }

                case true => {
                  val filesValue = mapOfFiles.getOrElse(fileName, (0L, 0L, 0L))
                  val prevLinesRead = filesValue._1
                  val prevModifiedTime = filesValue._2
                  val prevSize = filesValue._3
                  if (prevSize == fileSize && lastModifiedTime == prevModifiedTime) {
                    LOG.info("File exists in map of files, previous size of file is " + prevSize + " " + Thread
                      .currentThread().getName + " nothing to do, file remains unchanged " + fileName)

                  } else if (prevSize == fileSize && lastModifiedTime != prevModifiedTime) {
                    if (LOG.isDebugEnabled()) {
                      LOG.info("File exists in map of files, size is the same but lastModifiedTime changed " + Thread
                        .currentThread().getName +
                        " started processing modified file: " + fileName)
                    }
                    mapFileAvailability += (file -> false)
                    val linesRead = readStreamLines(file, 0L)
                    if (linesRead != 0) {
                      LOG.info("End processing modified file: " + fileName)
                      mapFileAvailability += (file -> true)
                    }
                  } else {
                    LOG.info("File exists in map of files, previous lines of file are " + prevLinesRead + " " + Thread
                      .currentThread().getName +
                      " started processing modified file: " + fileName)
                    mapFileAvailability += (file -> false)
                    val linesRead = readStreamLines(file, prevLinesRead)
                    if (linesRead != 0) {
                      LOG.info("End processing modified file: " + fileName)
                      mapFileAvailability += (file -> true)
                    } else {
                      LOG.info("lines read " + linesRead + " do nothing")
                    }
                  }
                }
              }
            }
          }
          executor.execute(thread)
        }

        case _ => LOG.error("Received event is not register.")
      }
    }
  }

  override def configure(context: Context): Unit = {
    sourceName = this.getName
    propertiesHelper = new PropertiesHelper(context, sourceName)
    sourceVFScounter = new SourceCounterVfs("SOURCE." + sourceName)
    LOG.info("Source " + sourceName + " watching path : " + propertiesHelper
      .getWorkingDirectory + " and pattern " + propertiesHelper.getPatternFilesMatch)

    if (propertiesHelper.getOutPutDirectory == "") {
      LOG.info("Property 'prcocess.dir', not set, files will not be moved after processing.")
    }

  }

  override def start(): Unit = {

    if (Files.exists(Paths.get(propertiesHelper.getStatusFile))) {
      mapOfFiles = loadMap(propertiesHelper.getStatusFile)
    }
    sourceVFScounter.start

    val fileObject = FileObjectBuilder.getFileObject(propertiesHelper.getWorkingDirectory)
    watchablePath = new WatchablePath(
      fileObject,
      listener,
      sourceName,
      propertiesHelper)

    super.start()

    if (propertiesHelper.getActionToTakeAfterProcessingFiles == "") {
      LOG.info("No action set for post-processing files from source is " + this.sourceName)
    } else if (propertiesHelper.getTimeoutPostProcess == SourceProperties.DEFAULT_TIMEOUT_POST_PROCESS_FILES) {
      LOG.warn("Action set for post-processing is " + propertiesHelper.getActionToTakeAfterProcessingFiles + " but " +
        "timeout for " + propertiesHelper
        .getActionToTakeAfterProcessingFiles + " files was not set via property " + SourceProperties
        .TIMEOUT_POST_PROCESS_FILES)
    } else {
      if (LOG.isDebugEnabled) {
        LOG.debug("Action set for post-processing is " + propertiesHelper.getActionToTakeAfterProcessingFiles)
      }
      try {
        service.scheduleWithFixedDelay(procFilesCollector, propertiesHelper.getInitialDelayPostProcess, propertiesHelper
          .getTimeoutPostProcess, TimeUnit.SECONDS)
      } catch {
        case ex: Throwable => {
          LOG.info("exception schedule " + ex)
        }
      }
    }
  }

  override def stop(): Unit = {
    saveMap(mapOfFiles, propertiesHelper.getStatusFile)
    sourceVFScounter.stop()

    //when reload by config avoid new filemonitor.
    watchablePath.getDefaultFilemonitor.stop()

    super.stop()
  }

  /**
    * Create Flume's event and send to Channel.
    *
    * @param data
    * @param fileName
    */
  def processMessage(data: Array[Byte], fileName: String): Unit = {
    val event: Event = new SimpleEvent
    val headers: java.util.Map[String, String] = new util.HashMap[String, String]()
    headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
    headers.put("fileName", fileName);
    event.setBody(data)
    event.setHeaders(headers)
    try {
      getChannelProcessor.processEvent(event)
      sourceVFScounter.incrementEventCount()
      sourceVFScounter.incrementCountSizeProc(data.size)
    } catch {
      case ex: ChannelException => {
        LOG.info("ChannelException was launched, putting to sleep.", ex)
        Thread.sleep(2000)
      }
    }
  }

  /**
    * Read inputstream skipping lines long
    *
    * @param file
    * @param linesLong
    * @return
    */

  def readStreamLines(file: FileObject, linesLong: Long): Long = {
    val filename = file.getName.getBaseName
    file.refresh()
    val inputStream = file.getContent.getInputStream

    if (inputStream == null) {
      return 0L
    }

    val in: BufferedReader = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()))

    var index = 0L

    while (index < linesLong && in.readLine() != null) {
      index += 1
    }

    var linesProcessed = 0L
    Stream.continually(in.readLine()).takeWhile(_ != null) foreach {
      in => {
        linesProcessed += 1
        processMessage(in.getBytes(), filename)
      }
    }
    in.close()

    if (LOG.isDebugEnabled) {
      LOG.debug("Lines read " + linesProcessed + " from line " + linesLong + " file: " + filename)
    }
    file.refresh()
    val lastModifiedTime = file.getContent.getLastModifiedTime
    val fileSize = file.getContent.getSize
    mapOfFiles += (filename -> (linesProcessed + linesLong, lastModifiedTime, fileSize))
    if (LOG.isDebugEnabled) {
      LOG.info("Save filename " + filename + " , " + linesProcessed + " , " + linesLong + " , " + lastModifiedTime
        + " , " + fileSize)
    }
    linesProcessed
  }

  /**
    * Write to file system a map of processed files
    *
    * @param mapOfFiles
    * @param statusFile
    * @return
    */
  def saveMap(mapOfFiles: mutable.Map[String, (Long, Long, Long)], statusFile: String):
  Boolean = {
    val oos = new ObjectOutputStream(new FileOutputStream(statusFile))
    try {
      oos.writeObject(mapOfFiles)
      oos.close()
      if (LOG.isDebugEnabled) {
        LOG.info("Write to map of files : " + statusFile)
      }
      true
    } catch {
      case io: IOException =>
        LOG.error("Cannot write object " + mapOfFiles + " to " + statusFile, io)
        false
    }
  }

  /**
    * Load from file system map of processed files.
    *
    * @param statusFile
    * @return
    */
  def loadMap(statusFile: String): mutable.HashMap[String, (Long, Long, Long)] = {
    try {
      val ois = new ObjectInputStream(new FileInputStream(statusFile))
      val mapOfFiles = ois.readObject().asInstanceOf[mutable.HashMap[String, (Long, Long, Long)]]
      ois.close()
      LOG.info("Load from file system map of processed files. " + statusFile)
      mapOfFiles
    } catch {
      case e: IOException =>
        LOG
          .warn("Map of files is could not be loaded because file is corrupted for source " + sourceName + " . " +
            "Generating new one.")
        new mutable.HashMap[String, (Long, Long, Long)]()
    }
  }

  /**
    * Move files to destiny under file system.
    *
    * @param processDir
    * @param file
    */
  def moveFile(processDir: String, file: FileObject): Unit = {
    val fileName = file.getName.getBaseName
    if (processDir != "" && FileObjectBuilder.getFileObject(propertiesHelper.getOutPutDirectory).exists()) {
      val fileDest: FileObject = FileObjectBuilder
        .getFileObject(propertiesHelper.getOutPutDirectory + System.getProperty("file.separator") + fileName)
      file.moveTo(fileDest)
      if (fileDest.exists()) {
        LOG
          .info("Moved processed file " + fileName + " to dir " + propertiesHelper
            .getOutPutDirectory)
      }
    } else {
      LOG.info("Action to take for post process file source " + this
        .sourceName + " is moving file " + fileName + " but no 'processed.dir' has been set or target directory not " +
        "exists. ")
    }
  }

  /**
    * Delete file from file system
    *
    * @param file
    */
  def deleteFile(file: FileObject): Unit = {
    val fileName = file.getName.getBaseName
    if (file.delete()) {
      if (LOG.isDebugEnabled) {
        LOG.debug("Deleted processed file " + fileName)
      }
    } else {
      LOG.error("Could not delete file after processing" + fileName)
    }
  }

  /**
    * Select what to do when file has been processed by flume.
    *
    * @param actionToTake
    * @param file
    */
  def postProcessFile(actionToTake: String, file: FileObject): Unit = {
    if (!SourceHelper
      .lastModifiedTimeExceededTimeout(file.getContent.getLastModifiedTime, propertiesHelper.getTimeoutPostProcess
        .toInt)) {
      actionToTake match {
        case "move" => moveFile(propertiesHelper.getOutPutDirectory, file)
        case "delete" => deleteFile(file)
        case _ =>
          LOG
            .error("For source " + this
              .sourceName + " action to take must be one of : move, delete, or just nothing to do, but wast set: " +
              actionToTake)
      }
    }
  }

  def getSourceName = sourceName

  def getSourceHelper = propertiesHelper

  def getSourceVfsCounter = sourceVFScounter

}





