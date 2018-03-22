package org.keedio.flume.source.vfs.filemonitor

import java.io.IOException
import java.nio.file._

import org.apache.commons.vfs2._
import org.apache.commons.vfs2.impl._
import org.junit._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by luislazaro on 7/9/15.
  * lalazaro@keedio.com
  * Keedio
  */

/**
  * DefaultMonitorTest just test the built-in functionality from
  * Apache-Commons Vfs2 DefaulMonitor
  *
  * @see https://commons.apache.org/proper/commons-vfs/apidocs/org/apache/commons/vfs2/impl/DefaultFileMonitor.html
  */

class DefaultMonitorTest {
  val LOG: Logger = LoggerFactory.getLogger(classOf[DefaultMonitorTest])
  /**
    * Test api for monitoring changes in directory. File system is local (file:). On starting the
    * DefaultMonitor instance, a file is create, this one is modified via append, and lastly the file is deleted,
    * with an interval of one second between actions. The listener receives the events and fires an action according
    * the event.
    */
  @Test
  def testApiFileMonitorLocalFileSystem(): Unit = {
    val tmp = System.getProperty("java.io.tmpdir")
    val dirTest = Files.createTempDirectory(Paths.get(tmp), this.getClass.getSimpleName)
    Assert.assertTrue(Files.exists(dirTest))
    val fsManager = VFS.getManager
    val directoryToBemMonitored: FileObject = fsManager.resolveFile(dirTest.toString)

    val fileMonitor = new DefaultFileMonitor(new FileListener {
      override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = {
        LOG.info("Received event create " + fileChangeEvent.getFile.getName)
        assert(true)
      }
      override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = {
        LOG.info("Received event modify " + fileChangeEvent.getFile.getName)
        assert(true)
      }
      override def fileDeleted(fileChangeEvent: FileChangeEvent): Unit = {
        LOG.info("Received event delete " + fileChangeEvent.getFile.getName)
        assert(true)
      }
    })

    fileMonitor.setRecursive(true)
    fileMonitor.addFile(directoryToBemMonitored)
    fileMonitor.setDelay(1) //if not set or set to 0 seconds, file changed is not fired so it is not detected.
    fileMonitor.start()

    try {
      val pathForFile = Files.createTempFile(dirTest, "file", ".txt")
      Assert.assertTrue(Files.exists(pathForFile))
      LOG.info("Creating file " + pathForFile)
      Thread.sleep(1000)

      Files.write(pathForFile, "content for file\n".getBytes(), StandardOpenOption.APPEND)
      LOG.info("Appending content to file " + pathForFile)
      Thread.sleep(1000)

      LOG.info("deleting file " + pathForFile)
      Files.deleteIfExists(pathForFile)
      Thread.sleep(1000)

      LOG.info("Cleaning " + dirTest)
      Files.delete(dirTest)
      Assert.assertFalse(Files.exists(dirTest))
      fileMonitor.stop()

    } catch {
      case e: IOException => LOG.error("I/O: ", e)
    }
  }
}