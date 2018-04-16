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
  var mapOfFiles = mutable.HashMap[String, Long]()
  var sourceVFScounter = new org.keedio.flume.source.vfs.metrics.SourceCounterVfs("")
  val executor: ExecutorService = Executors.newFixedThreadPool(10)
  var sourceName: String = ""
  var statusFile = ""
  var workDir: String = ""
  var includePattern: String = ""
  var processedDir: String = ""
  var processDiscovered: Boolean = true
  var timeOut: Int = 0

  val listener = new StateListener {
    override def statusReceived(event: StateEvent): Unit = {
      event.getState.toString() match {
        case "entry_create" => {
          val thread = new Thread() {
            override def run(): Unit = {
              val file: FileObject = event.getFileChangeEvent.getFile
              val fileName = file.getName.getBaseName
              if (mapOfFiles.contains(fileName)) {
                LOG.info("File was already processed do nothing !  " + fileName)
              } else {
                LOG.info("Source " + sourceName + " received event: " + event.getState
                  .toString() + " file " + fileName)
                val fileSize = event.getFileChangeEvent.getFile.getContent.getSize
                val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream
                LOG.info(Thread.currentThread().getName + " started processing new file: " + fileName)
                if (readStream(inputStream, fileName, 0)) {
                  LOG.info("End processing new file: " + fileName)
                  mapOfFiles += (fileName -> fileSize)
                  saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
                  sourceVFScounter.incrementFilesCount()
                  sourceVFScounter.incrementCountSizeProc(fileSize)
                  movFile(processedDir, file)
                }
              }
            }
          }
          executor.execute(thread)
        } //entry_create

        case "entry_modify" => {
          val thread = new Thread() {
            override def run(): Unit = {
              val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
              LOG.info("Source " + sourceName + " received event: " + event.getState
                .toString() + " file " + fileName)
              val fileSize = event.getFileChangeEvent.getFile.getContent.getSize
              val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream
              val prevSize = mapOfFiles.getOrElse(fileName, 0L)
              LOG.info("File exists in map of files, previous size of file is " + prevSize + ", " + Thread
                .currentThread().getName + " started processing modified file: " + fileName)
              if (readStream(inputStream, fileName, prevSize)) {
                LOG.info("End processing modified file: " + fileName)
                mapOfFiles -= fileName
                mapOfFiles += (fileName -> fileSize)
                saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
              }
            }
          }
          executor.execute(thread)
        } //entry_modify

        case "entry_delete" => {
          val thread: Thread = new Thread() {
            override def run(): Unit = {
              val file: FileObject = event.getFileChangeEvent.getFile
              val fileName = file.getName.getBaseName
              LOG.info("Source " + sourceName + " received event: " + event.getState
                .toString() + " file " + fileName)
            }
          }
          executor.execute(thread)
        } //entry_delete

        case "entry_discover" => {
          val thread = new Thread() {
            override def run(): Unit = {
              val file: FileObject = event.getFileChangeEvent.getFile
              val fileName = file.getName.getBaseName
              LOG.info("Source " + sourceName + " received event: " + event.getState
                .toString() + " file " + fileName)
              val fileSize = event.getFileChangeEvent.getFile.getContent.getSize
              val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream
              mapOfFiles.get(fileName).isDefined match {
                case false =>
                  LOG.info(Thread.currentThread().getName + " started processing file discovered: " + fileName)
                  if (readStream(inputStream, fileName, 0)) {
                    LOG.info("End processing discovered file: " + fileName)
                    mapOfFiles += (fileName -> fileSize)
                    saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
                    sourceVFScounter.incrementFilesCount()
                    sourceVFScounter.incrementCountSizeProc(fileSize)
                    movFile(processedDir, file)
                  }

                case true => {
                  val prevSize = mapOfFiles.getOrElse(fileName, fileSize)
                  if (prevSize == fileSize) {
                    LOG.info("File exists in map of files, previous size of file is " + prevSize + " " + Thread
                      .currentThread().getName + " nothing to do, file remains unchanged " + fileName)
                    Unit
                  } else {
                    LOG.info("File exists in map of files, previous size of file is " + prevSize + " " + Thread
                      .currentThread().getName +
                      " started processing modified file: " + fileName)
                    if (readStream(inputStream, fileName, prevSize)) {
                      LOG.info("End processing modified file: " + fileName)
                      mapOfFiles -= fileName
                      mapOfFiles += (fileName -> fileSize)
                      saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
                    }
                  }
                  movFile(processedDir, file)
                }
              }
            }
          }
          executor.execute(thread)
        } //entry_discover

        case _ => LOG.error("Recieved event is not register.")
      }
    }
  }

  override def configure(context: Context): Unit = {
    sourceName = this.getName
    sourceVFScounter = new SourceCounterVfs("SOURCE." + sourceName)
    workDir = context.getString("work.dir")
    includePattern = context.getString("includePattern", """[^.]*\.*?""")
    LOG.info("Source " + sourceName + " watching path : " + workDir + " and pattern " + includePattern)
    processedDir = context.getString("processed.dir", "")
    if (processedDir == "") {
      LOG.info("Property 'prcocess.dir', not set, files will not be moved after processing.")
    }

    statusFile = Paths
      .get(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + sourceName + ".ser").toString

    processDiscovered = context.getString("process.discovered.files", "true").toBoolean

    timeOut = context.getString("timeout.start.process", "0").toInt

    if (Files.exists(Paths.get(statusFile))) {
      mapOfFiles = loadMap(statusFile)
    }
  }

  override def start(): Unit = {
    super.start()
    sourceVFScounter.start
    val fileObject: FileObject = FileObjectBuilder.getFileObject(workDir)
    val watchable = new WatchablePath(
      workDir,
      5,
      2,
      s"""$includePattern""".r,
      fileObject,
      listener,
      processDiscovered,
      sourceName,
      timeOut)
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

    Stream.continually(in.readLine()).takeWhile(_ != null) foreach {
      in => processMessage(in.getBytes(), filename)
    }

    in.close()
    true
  }

  /**
    * Write to file system a map of processed files.
    *
    * @param mapOfFiles
    * @param statusFile
    * @return
    */
  def saveMap(mapOfFiles: mutable.Map[String, Long], statusFile: String, fileName: String, state: String): Boolean = {
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
  def loadMap(statusFile: String): mutable.HashMap[String, Long] = {
    val ois = new ObjectInputStream(new FileInputStream(statusFile))
    val mapOfFiles = ois.readObject().asInstanceOf[mutable.HashMap[String, Long]]
    ois.close()
      LOG.info("Load from file system map of processed files. " + statusFile)
    mapOfFiles
  }

  /**
    * Move files to destiny under file system.
    * @param processDir
    * @param file
    */
  def movFile(processDir: String, file: FileObject): Unit = {
    val fileName = file.getName.getBaseName
    if (processDir != "" && FileObjectBuilder.getFileObject(processedDir).exists()) {
      val fileDest: FileObject = FileObjectBuilder.getFileObject(processedDir + "/" + fileName)
      file.moveTo(fileDest)
      if (fileDest.exists()) {
        LOG.info("Moving processed file " + fileName + " to dir " + processedDir)
      }
    }
  }

}
