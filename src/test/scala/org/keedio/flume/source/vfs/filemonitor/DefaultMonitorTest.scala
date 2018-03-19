package org.keedio.flume.source.vfs.filemonitor

import java.io.IOException
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.commons.vfs2._
import org.apache.commons.vfs2.impl._
import org.junit._

/**
 * Created by luislazaro on 7/9/15.
 * lalazaro@keedio.com
 * Keedio
 */

/**
 * DefaultMonitorTest just test the built-in functionality from
 * Apache-Commons Vfs2 DefaulMonitor
 * @see https://commons.apache.org/proper/commons-vfs/apidocs/org/apache/commons/vfs2/impl/DefaultFileMonitor.html
 */

class DefaultMonitorTest {

      /**
       * Test api for monitoring changes in directory. The filesystem uses is local file system. On starting the
       * DefaultMonitor instance, a file is create, this one is modified via append, and lastly the file is deletec,
       * with an interval of one second between actions. The listener receives the events and fires an action according
       * the event.
       */
      @Test
      def testApiFileMonitorLocalFileSystem(): Unit = {
            val userPath = System.getProperty("user.dir")
            val fileSeparator = System.getProperty("file.separator")
            val fsManager = VFS.getManager
            val listendir: FileObject = fsManager.resolveFile(userPath + fileSeparator + "src/test/resources/")

            val fm = new DefaultFileMonitor(new FileListener {
                  override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = assert(true)

                  override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = assert(true)

                  override def fileDeleted(fileChangeEvent: FileChangeEvent): Unit = assert(true)

            })

            fm.setRecursive(true)
            fm.addFile(listendir)
            fm.setDelay(1) //if not set or set to 0 seconds, file changed is not fired so it is not detected.
            fm.start()

            try {
                  Files.createFile(Paths.get(userPath + fileSeparator + "src/test/resources/file_Created1.csv"))
                  Thread.sleep(1000)
                  Files.write(Paths.get(userPath + fileSeparator + "src/test/resources/file_Created1.csv"), ("Contenido de" +
                        " fichero\n").getBytes(), StandardOpenOption.APPEND);
                  Thread.sleep(1000)
                  Files.deleteIfExists(Paths.get(userPath + fileSeparator + "src/test/resources/file_Created1.csv"))
                  Thread.sleep(1000)
            } catch {
                  case e: IOException => println("I/O: ", e)
            }
            fm.stop()
      }

}