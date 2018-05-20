package org.keedio.flume.source.vfs.source

import java.io._
import java.nio.charset.Charset
import java.nio.file._
import java.util
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.commons.vfs2.FileObject
import org.apache.flume.conf.Configurable
import org.apache.flume.event.SimpleEvent
import org.apache.flume.source.AbstractSource
import org.apache.flume.{ChannelException, Context, Event, EventDrivenSource}
import org.keedio.flume.source.vfs.config.{SourceHelper, SourceProperties}
import org.keedio.flume.source.vfs.metrics.SourceCounterVfs
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
  private var mapOfFiles = mutable.HashMap[String, (Long, Long, Long, String)]()
  private var sourceVFScounter = new org.keedio.flume.source.vfs.metrics.SourceCounterVfs("")
  private val executor: ExecutorService = Executors.newFixedThreadPool(10)
  private var sourceName: String = ""
  private var sourceHelper: SourceHelper = _
  private var totalLines: Long = 0L

  val listener = new StateListener {
    override def statusReceived(event: StateEvent): Unit = {
      event.getState.toString() match {
        case "entry_create" => {
          val thread = new Thread() {
            override def run(): Unit = {
              val file: FileObject = event.getFileChangeEvent.getFile
              val fileName = file.getName.getBaseName
              val fileContent = file.getContent
              val inputStream = fileContent.getInputStream
              val fileSize = fileContent.getSize
              if (mapOfFiles.contains(fileName)) {
                LOG
                  .info("File  " + fileName + " was already processed, do nothing !. If desired behavior is to " +
                    "reprocess "
                    + "file, set property " + SourceProperties
                    .RETAIN_DELETED_FILES_IN_MAP_WHEN_POSTPROCESS + " to false (or just remove property).")
              } else {
                LOG.info("Source " + sourceName + " received event: " + event.getState
                  .toString() + " file " + fileName)
                LOG.info(Thread.currentThread().getName + " started processing new file: " + fileName)
                mapOfFiles += (fileName -> (0L, 0L, 0L, event.getState.toString()))
                val linesRead =  readStreamLines(inputStream, fileName, 0L)
                if (linesRead != 0) {
                  LOG.info("End processing new file: " + fileName)
                  val lastModifiedTime = file.getContent.getLastModifiedTime
                  mapOfFiles += (fileName -> (linesRead, lastModifiedTime, fileSize, "available"))
                  saveMap(mapOfFiles, sourceHelper.getStatusFile, fileName, event.getState.toString())
                  sourceVFScounter.incrementFilesCount()
                  sourceVFScounter.incrementCountSizeProc(fileSize)
                  postProcessFile(sourceHelper.getActionToTakeAfterProcessingFiles, file)
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
              val inputStream = fileContent.getInputStream
              val fileSize = fileContent.getSize
              val lastModifiedTime = file.getContent.getLastModifiedTime
              LOG.info("Source " + sourceName + " received event: " + event.getState
                .toString() + " file " + fileName)
              val filesValue = mapOfFiles.getOrElse(fileName, (0L, 0L, 0L, "available"))
              val prevLinesRead = filesValue._1
              val prevModifiedTime = filesValue._2
              val prevSize = filesValue._3
              val available: String = filesValue._4
              available match {

                case "available" =>

                  if (prevSize == fileSize && lastModifiedTime == prevModifiedTime) {
                    LOG.info("File exists in map of files, previous size of file is " + prevSize + " " + Thread
                      .currentThread().getName + " nothing to do, file remains unchanged " + fileName)
                    Unit
                  } else if (prevSize == fileSize && lastModifiedTime != prevModifiedTime) {
                    LOG.info("File exists in map of files, size is the same but lastModifiedTime changed " + Thread
                      .currentThread().getName +
                      " started processing modified file: " + fileName)
                    if (available == "available") {
                      mapOfFiles += (fileName -> (0L, 0L, 0L, event.getState.toString()))
                      if (readStream(inputStream, fileName, 0L)) {
                        LOG.info("End processing modified file: " + fileName)
                        mapOfFiles -= fileName
                        mapOfFiles += (fileName -> (0L, lastModifiedTime, fileSize, "available"))
                        saveMap(mapOfFiles, sourceHelper.getStatusFile, fileName, event.getState.toString())
                      }
                    }
                  } else {
                    val aux = fileSize > prevSize match {
                      case true => prevLinesRead
                      case false => 0L
                    }
                    LOG.info("File exists in map of files, previous lines of file are " + prevLinesRead + " " + Thread
                      .currentThread().getName +
                      " started processing modified file: " + fileName)

                    if (available == "available") {
                      mapOfFiles += (fileName -> (0L, 0L, 0L, event.getState.toString()))
                      val linesRead = readStreamLines(inputStream, fileName, aux)
                      if (linesRead != 0) {
                        LOG.info("End processing modified file: " + fileName)
                        mapOfFiles -= fileName
                        mapOfFiles += (fileName -> (linesRead + prevLinesRead, lastModifiedTime, fileSize, "available"))
                        saveMap(mapOfFiles, sourceHelper.getStatusFile, fileName, event.getState.toString())
                      } else {
                        LOG.info("lines read " + linesRead + " do nothing")
                      }
                    }
                  }
                case _ => LOG.info( "status " + available + " filename " + fileName)
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
              if (!sourceHelper.isKeepFilesInMap) {
                mapOfFiles -= fileName
              }
              LOG.info("Source " + sourceName + " received event: " + event.getState
                .toString() + " file " + fileName)
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
              val inputStream = file.getContent.getInputStream
              LOG.info("Source " + sourceName + " received event: " + event.getState
                .toString() + " file " + fileName)
              mapOfFiles.get(fileName).isDefined match {
                case false =>
                  LOG.info(Thread.currentThread().getName + " started processing file discovered: " + fileName)
                  val linesRead = readStreamLines(inputStream, fileName, 0L)
                  if (linesRead != 0) {
                    LOG.info("End processing discovered file: " + fileName)
                    mapOfFiles += (fileName -> (linesRead, lastModifiedTime, fileSize, "available"))
                    saveMap(mapOfFiles, sourceHelper.getStatusFile, fileName, event.getState.toString())
                    sourceVFScounter.incrementFilesCount()
                    sourceVFScounter.incrementCountSizeProc(fileSize)
                    postProcessFile(sourceHelper.getActionToTakeAfterProcessingFiles, file)
                  }

                case true => {
                  val filesValue = mapOfFiles.getOrElse(fileName, (0L, 0L, 0L, "available"))
                  val prevLinesRead = filesValue._1
                  val prevModifiedTime = filesValue._2
                  val prevSize = filesValue._3
                  if (prevSize == fileSize && lastModifiedTime == prevModifiedTime) {
                    LOG.info("File exists in map of files, previous size of file is " + prevSize + " " + Thread
                      .currentThread().getName + " nothing to do, file remains unchanged " + fileName)
                    Unit
                  } else if (prevSize == fileSize && lastModifiedTime != prevModifiedTime) {
                    LOG.info("File exists in map of files, size is the same but lastModifiedTime changed " + Thread
                      .currentThread().getName +
                      " started processing modified file: " + fileName)
                    if (readStream(inputStream, fileName, 0L)) {
                      LOG.info("End processing modified file: " + fileName)
                      mapOfFiles -= fileName
                      mapOfFiles += (fileName -> (0L, fileSize, lastModifiedTime, "available"))
                      saveMap(mapOfFiles, sourceHelper.getStatusFile, fileName, event.getState.toString())
                    }
                  } else {
                    LOG.info("File exists in map of files, previous lines of file are " + prevLinesRead + " " + Thread
                      .currentThread().getName +
                      " started processing modified file: " + fileName)
                    val linesRead = readStreamLines(inputStream, fileName, prevLinesRead)
                    if (linesRead != 0) {
                      LOG.info("End processing modified file: " + fileName)
                      mapOfFiles -= fileName
                      mapOfFiles += (fileName -> (linesRead + prevLinesRead, lastModifiedTime, fileSize, "available"))
                      saveMap(mapOfFiles, sourceHelper.getStatusFile, fileName, event.getState.toString())
                    } else {
                      LOG.info("lines read " + linesRead + " do nothing")
                    }
                    postProcessFile(sourceHelper.getActionToTakeAfterProcessingFiles, file)
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
    sourceHelper = new SourceHelper(context, sourceName)
    sourceVFScounter = new SourceCounterVfs("SOURCE." + sourceName)
    LOG.info("Source " + sourceName + " watching path : " + sourceHelper
      .getWorkingDirectory + " and pattern " + sourceHelper.getPatternFilesMatch)

    if (sourceHelper.getOutPutDirectory == "") {
      LOG.info("Property 'prcocess.dir', not set, files will not be moved after processing.")
    }

    if (Files.exists(Paths.get(sourceHelper.getStatusFile))) {
      mapOfFiles = loadMap(sourceHelper.getStatusFile)
    }

  }

  override def start(): Unit = {
    super.start()
    sourceVFScounter.start
    val fileObject: FileObject = FileObjectBuilder.getFileObject(sourceHelper.getWorkingDirectory)
    val watchable = new WatchablePath(
      2,
      2,
      fileObject,
      listener,
      sourceName,
      sourceHelper)
  }

  override def stop(): Unit = {
    sourceVFScounter.stop()
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
    } catch {
      case ex: ChannelException => {
        LOG.info("ChannelException was launched, putting to sleep.", ex)
        Thread.sleep(2000)
      }
    }
  }

  /**
    * Read retrieved stream from source into byte[] and process by flume
    *
    * @param inputStream
    * @param filename
    * @param size
    * @return
    */
  def readStream(inputStream: InputStream, filename: String, size: Long): Boolean = {
    if (inputStream == null) {
      return false
    }
    inputStream.skip(size)
    val in: BufferedReader = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()))
    var lines = 0

    Stream.continually(in.readLine()).takeWhile(_ != null) foreach {
      in => {
        lines += 1
        processMessage(in.getBytes(), filename)
      }
    }
    totalLines += lines
    LOG.info("Total lines = " + lines + " per block , total is  " + totalLines)
    in.close()
    true
  }

  /**
  * Read inputstream skipping lines long
 *
  * @param inputStream
  * @param filename
  * @param linesLong
  * @return
  */

  def readStreamLines(inputStream: InputStream, filename: String, linesLong: Long): Long = {
    if (inputStream == null) {
      return 0L
    }

    var lines = 0L
    var index = 0L

    val in: BufferedReader = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()))

   while(index < linesLong  && in.readLine() != null) {
     index += 1
   }

    Stream.continually(in.readLine()).takeWhile(_ != null) foreach {
      in => {
        lines += 1
        processMessage(in.getBytes(), filename)
      }
    }

    in.close()

    if (LOG.isDebugEnabled) {
      LOG.debug("Lines read " + lines + " from file: " + filename)
    }

    lines
  }

  /**
    * Write to file system a map of processed files.
    *
    * @param mapOfFiles
    * @param statusFile
    * @return
    */
  def saveMap(mapOfFiles: mutable.Map[String, (Long, Long, Long, String)], statusFile: String, fileName: String,
              state: String):
  Boolean = {
    val oos = new ObjectOutputStream(new FileOutputStream(statusFile))
    try {
      oos.writeObject(mapOfFiles)
      oos.close()
      LOG.info("Write to map of files " + state + ": " + fileName)
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
  def loadMap(statusFile: String): mutable.HashMap[String, (Long, Long, Long, String)] = {
    try {
      val ois = new ObjectInputStream(new FileInputStream(statusFile))
      val mapOfFiles = ois.readObject().asInstanceOf[mutable.HashMap[String, (Long, Long, Long, String)]]
      ois.close()
      LOG.info("Load from file system map of processed files. " + statusFile)
      mapOfFiles
    } catch {
      case e: IOException =>
        LOG
          .warn("Map of files is could not be loaded because file is corrupted for source " + sourceName + " . " +
            "Generating new one.")
        new mutable.HashMap[String, (Long, Long, Long, String)]()
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
    if (processDir != "" && FileObjectBuilder.getFileObject(sourceHelper.getOutPutDirectory).exists()) {
      val fileDest: FileObject = FileObjectBuilder
        .getFileObject(sourceHelper.getOutPutDirectory + System.getProperty("file.separator") + fileName)
      file.moveTo(fileDest)
      if (fileDest.exists()) {
        LOG
          .info("Moving processed file " + fileName + " to dir " + sourceHelper
            .getOutPutDirectory + " by action to take for post " +
            "process file is move.")
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
      LOG.info("Deleting processed file " + fileName + " by action to take for post process file is delete.")
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
    actionToTake.trim match {
      case "" =>
        LOG
          .info("No action set for post-processing files source is " + this.sourceName + " nothing to do with " + file
            .getName.getBaseName)
        ()
      case _ => actionToTake match {
        case "move" => moveFile(sourceHelper.getOutPutDirectory, file)
        case "delete" => deleteFile(file)
        case _ =>
          LOG
            .error("For source " + this
              .sourceName + " action to take must be one of : move, delete, or just nothing to do, but wast set: " +
              actionToTake)
          ()
      }
    }
  }

  def getSourceName = sourceName

  def getSourceHelper = sourceHelper

  def getSourceVfsCounter = sourceVFScounter

}
