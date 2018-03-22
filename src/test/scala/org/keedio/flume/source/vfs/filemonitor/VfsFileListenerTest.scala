package org.keedio.flume.source.vfs.filemonitor

import java.io.IOException
import java.nio.file.{Files, Paths}

import org.apache.commons.vfs2.impl.DefaultFileMonitor
import org.apache.commons.vfs2._
import org.junit.{Assert, Test}
import org.keedio.flume.source.vfs.watcher.VfsFileListener
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by luislazaro on 20/3/18.
  * lalazaro@keedio.com
  * Keedio
  */
class VfsFileListenerTest {
  val LOG: Logger = LoggerFactory.getLogger(classOf[VfsFileListenerTest])
  /**
    * Test VfsFileListener implementing FileListener.
    * If any file exists before DefaulMonitor starts, it must be discovered and listeners must be noticed.
    * The provided solution is not operating with the cache, but iterating over fileMonitor.getChildren and triggering
    * event discover for each children.
    */
  @Test
  def testApiFileMonitroLocalFileSystemWithVFSListener(): Unit =  {
    val tmp = System.getProperty("java.io.tmpdir")
    val dirTest = Files.createTempDirectory(Paths.get(tmp), this.getClass.getSimpleName)
    Assert.assertTrue(Files.exists(dirTest))
    val fsManager = VFS.getManager
    val directoryToBemMonitored: FileObject = fsManager.resolveFile(dirTest.toString)

    val vfsListener = new VfsFileListener {
      override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = ()
      override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = ()
      override def fileDeleted(fileChangeEvent: FileChangeEvent): Unit = ()
      override def fileDiscovered(event: FileChangeEvent): Unit = {
        LOG.info("Received event discover " + event.getFile.getName)
        assert(true)
      }

    }
    val fileMonitor = new DefaultFileMonitor(vfsListener)
    fileMonitor.setRecursive(true)
    fileMonitor.addFile(directoryToBemMonitored)
    fileMonitor.setDelay(1) //if not set or set to 0 seconds, file changed is not fired so it is not detected.


    try {
      val pathForFile = Files.createTempFile(dirTest, "file", ".txt")
      LOG.info("Creating file " + pathForFile)
      Assert.assertTrue(Files.exists(pathForFile))

      LOG.info("Starting fileMonitor")
      fileMonitor.start()
      directoryToBemMonitored.getChildren.foreach( child => vfsListener.fileDiscovered(new FileChangeEvent(child)))
      Thread.sleep(1000)

      LOG.info("Cleaning " + dirTest)
      Files.deleteIfExists(pathForFile)
      Files.delete(dirTest)
      Assert.assertFalse(Files.exists(dirTest))
      fileMonitor.stop()
    } catch {
      case e: IOException => LOG.error("I/O: ", e)
    }
  }

}
