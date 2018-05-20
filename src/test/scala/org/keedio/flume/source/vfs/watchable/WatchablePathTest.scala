package org.keedio.flume.source.vfs.watchable

import java.io.IOException
import java.nio.file._
import java.util.{Calendar, Date}

import org.apache.commons.vfs2._
import org.apache.commons.vfs2.impl.StandardFileSystemManager
import org.junit._
import org.keedio.flume.source.vfs.config.SourceHelper
import org.keedio.flume.source.vfs.watcher._
import org.mockito.Mockito
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex

/**
  * Created by luislazaro on 9/9/15.
  * lalazaro@keedio.com
  * Keedio
  */
class WatchablePathTest {

  val LOG: Logger = LoggerFactory.getLogger(classOf[WatchablePathTest])
  val csvRegex: Regex = """[^.]*\.csv""".r
  val csvDir = System.getProperty("java.io.tmpdir")

  /**
    * For 20 seconds (10 iterations * 2 seconds) and every
    * 2 seconds, csv's directory will be checked. Each two iterations
    * and action will be taken over the files, i.e, delete file, append file,
    * create file. According the action a event will be fired.
    */
  @Test
  def testWatchPath(): Unit = {
    println("##### testWatchPath : monitor directory a send events according actions  ")
    val refreshTime = 2
    val startTime = 2
    val fs = new StandardFileSystemManager
    fs.setCacheStrategy(CacheStrategy.MANUAL)
    fs.init()

    val listener = new StateListener {
      override def statusReceived(event: StateEvent): Unit = {
        Assert.assertTrue(true)
        LOG.info("listener received event: " + event.getState.toString()
          + " on element " + event.getFileChangeEvent.getFile.getName)
      }
    }

    val sourceHelper = Mockito.mock(classOf[SourceHelper])
    Mockito.when(sourceHelper.getWorkingDirectory).thenReturn(csvDir)
    Mockito.when(sourceHelper.getPatternFilesMatch).thenReturn(csvRegex)

    val watchable = new WatchablePath(fs
      .resolveFile(csvDir), listener, "sourcename", sourceHelper)
    conditionsGenerator(10, 2000) //(10 iterations * 2 seconds)

  }

  @Test
  def testWatchPathWithDiscover = {
    println("##### testWatchPathWithDiscover : monitor directory a send events according actions  ")
    println("Action taken is creating new files before start watching")
    try {
      for (i <- 1 to 5) {
        Files.createFile(Paths.get(s"$csvDir" + s"/file_Discovered${i}.csv"))
      }

    } catch {
      case e: IOException => LOG.error("I/O: when creating file", e)
        assert(false)
    }

    val refreshTime = 2
    val startTime = 2
    val fs = new StandardFileSystemManager
    fs.setCacheStrategy(CacheStrategy.MANUAL)
    fs.init()

    val listener = new StateListener {
      override def statusReceived(event: StateEvent): Unit = {
        Assert.assertTrue(true)
        LOG.info("listener received event: " + event.getState.toString()
          + " on element " + event.getFileChangeEvent.getFile.getName)
      }
    }

    val sourceHelper = Mockito.mock(classOf[SourceHelper])
    Mockito.when(sourceHelper.getWorkingDirectory).thenReturn(csvDir)
    Mockito.when(sourceHelper.getPatternFilesMatch).thenReturn(csvRegex)

    val watchable = new WatchablePath( fs
      .resolveFile(csvDir), listener, "sourcename", sourceHelper)

    try {
      for (i <- 1 to 5) {
        Files.deleteIfExists(Paths.get(s"$csvDir" + s"/file_Discovered${i}.csv"))
      }

    } catch {
      case e: IOException => LOG.error("I/O: when deleting file", e)
        assert(false)
    }
  }

  /**
    * Take actions over a directory to produce a response over time
    *
    * @param iterations
    * @param timeToSleep
    */
  def conditionsGenerator(iterations: Int, timeToSleep: Long): Unit = {
    for (i <- 1 to iterations) {
      Thread.sleep(timeToSleep)
      println("iteration " + i)
      i match {
        case 3 =>
          println("Action taken is creating new files")
          try {
            for (i <- 1 to 5) {
              Files.createFile(Paths.get(s"$csvDir" + s"/file_Created${i}.csv"))
            }

          } catch {
            case e: IOException => LOG.error("I/O: conditionsGenerator", e)
              assert(false)
          }

        case 5 =>
          println("Action taken is appending to files")
          try {
            for (i <- 1 to 5) {
              Files.write(Paths.get(s"$csvDir" + s"/file_Created${i}.csv"),
                (getCurrentDate + "\n").getBytes(),
                StandardOpenOption.APPEND
              )
            }

          } catch {
            case e: IOException => LOG.error("I/O: conditionsGenerator", e)
              assert(false)
          }

        case 7 =>
          println("Action taken is deleting files")
          try {
            for (i <- 1 to 5) {
              Files.deleteIfExists(Paths.get(s"$csvDir" + s"/file_Created${i}.csv"))
            }

          } catch {
            case e: IOException => LOG.error("I/O: conditionsGenerator", e)
              assert(false)
          }

        case 10 => println("end")
          try {
            Files.deleteIfExists(Paths.get(s"$csvDir" + s"/file1.csv"))
          } catch {
            case e: IOException => LOG.error("I/O: conditionsGenerator", e)
              assert(false)
          }
        case _ => ()

      }

    }
  }

  /**
    * get current date
    *
    * @return
    */
  def getCurrentDate: String = {
    val today: Date = Calendar.getInstance().getTime()
    today.toString + System.currentTimeMillis()
  }

}
