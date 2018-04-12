package org.keedio.flume.source.vfs.fileSystems

import java.nio.file._
import java.nio.file.attribute.FileTime
import java.util
import java.util.{Calendar, Date}

import org.apache.flume._
import org.apache.flume.channel._
import org.apache.flume.conf.Configurables
import org.apache.flume.lifecycle.{LifecycleController, LifecycleState}
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers._
import org.junit.{After, _}
import org.keedio.flume.source.vfs.source.SourceVFS
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by luislazaro on 23/3/18.
  * lalazaro@keedio.com
  * Keedio
  */
class LocalFileTest {

  val LOG: Logger = LoggerFactory.getLogger(classOf[LocalFileTest])
  val tmp = System.getProperty("java.io.tmpdir")
  var source: SourceVFS = _
  var tmpDir: Path = _
  var channel: MemoryChannel = _

  @Before
  def setUp(): Unit = {
    source = new SourceVFS()
    source.setName("LocalFileTest")
    channel = new MemoryChannel
    Configurables.configure(channel, new Context)

    val channels = new util.ArrayList[Channel]()
    channels.add(channel)

    val rcs: ChannelSelector = new ReplicatingChannelSelector
    rcs.setChannels(channels)
    source.setChannelProcessor(new ChannelProcessor(rcs))

    val tmp = System.getProperty("java.io.tmpdir")
    tmpDir = Files.createTempDirectory(Paths.get(tmp), "incoming")
  }

  @After
    def tearDown(): Unit = {
    import scala.collection.JavaConversions._
      Files.newDirectoryStream(tmpDir).foreach(path => {
      LOG.info("Cleaning file " + path)
      Files.deleteIfExists(path)
    })
    Files.deleteIfExists(tmpDir)
    Files.deleteIfExists(Paths.get(source.statusFile))
    source.stop
  }

  @Test
  def testLifecycle = {

    val context = new Context()
    context.put("work.dir", tmpDir.toString)
    context.put("process.discovered.files", "true")
    context.put("timeout.files", "0")

    Configurables.configure(source, context)

    source.start()
    Assert.assertTrue("Reached start or error", LifecycleController.waitForOneOf(
      source,
      LifecycleState.START_OR_ERROR
    ))
    Assert.assertEquals("Server is started", LifecycleState.START, source.getLifecycleState())
  }

  @Test
  def testPutHeaders = {
    val file = Files.createTempFile(tmpDir, "file1", ".txt")
    Files.write(file, "fileline1\nfileline2\n".getBytes(), StandardOpenOption.APPEND)

    val context = new Context()
    context.put("work.dir", tmpDir.toString)
    context.put("process.discovered.files", "true")
    context.put("timeout.files", "0")
    Configurables.configure(source, context)

    source.start()
    val txn: Transaction = channel.getTransaction
    txn.begin()
    val event = channel.take()
    txn.commit()
    txn.close()

    Assert.assertNotNull(event.getHeaders.get("fileName"))
    Assert.assertNotNull(event.getHeaders.get("timestamp"))
    Assert.assertEquals(file.getFileName.toString, event.getHeaders.get("fileName"))
  }

  /**
    * Source starts and there is already a file under monitored directory.
    * An "entry_discovered" must be fired.
    */
  @Test
  def testProcessDiscoveredSingleFile = {
    val file = Files.createTempFile(tmpDir, "file1", ".txt")
    Files.write(file, "fileline1\nfileline2\n".getBytes(), StandardOpenOption.APPEND)

    val context = new Context()
    context.put("work.dir", tmpDir.toString)
    context.put("process.discovered.files", "true")
    context.put("timeout.files", "0")
    Configurables.configure(source, context)

    source.start()
    val txn: Transaction = channel.getTransaction
    txn.begin()
    (1 to 2).toList.foreach(i => {
      val event = channel.take()
      LOG.info("event " + i + " body contains " + new String(event.getBody))
    })

    txn.commit()
    txn.close()
    Thread.sleep(1000)
    Assert.assertEquals(source.sourceVFScounter.getEventCount, 2)
    Assert.assertEquals(source.sourceVFScounter.getFilesCount, 1)

  }

  /**
    * Source starts and a file is created. It must be triggered an "entry_create"
    */
  @Test
  def testProcessCreatedSingleFile = {
    val context = new Context()
    context.put("work.dir", tmpDir.toString)
    context.put("process.discovered.files", "false")
    context.put("timeout.files", "0")
    Configurables.configure(source, context)
    source.start()

    val file: Path = Files.createTempFile(tmpDir, "file1", ".txt")
    Files.write(file, "fileline1\nfileline2\n".getBytes(), StandardOpenOption.APPEND)
    LOG.info("create file " + file)

    val txn: Transaction = channel.getTransaction
    txn.begin()

    (1 to 2).toList.foreach(i => {
      val event = channel.take()
      Assert.assertEquals(new String(event.getBody), "fileline" + i)
      LOG.info("event " + i + " body contains " + new String(event.getBody))
    })

    txn.commit()
    txn.close()

    Thread.sleep(1000)
    Assert.assertEquals(source.sourceVFScounter.getEventCount, 2)
    Assert.assertEquals(source.sourceVFScounter.getFilesCount, 1)

  }

  @Test
  def testProcessModifiedFiles = {
    val context = new Context()
    context.put("work.dir", tmpDir.toString)
    context.put("process.discovered.files", "false")
    context.put("timeout.files", "0")
    Configurables.configure(source, context)
    source.start()

    val file: Path = Files.createTempFile(tmpDir, "file1", ".txt")
    Files.write(file, "fileline1\n".getBytes(), StandardOpenOption.APPEND)
    LOG.info("create file " + file)

    val txn: Transaction = channel.getTransaction
    txn.begin()

    val event1 = channel.take()
    Assert.assertEquals(new String(event1.getBody), "fileline1")
    LOG.info("event body contains " + new String(event1.getBody))

    Thread.sleep(1000)

    Files.write(file, "fileline2\n".getBytes(), StandardOpenOption.APPEND)
    LOG.info("append to file " + file)
    Thread.sleep(5000)
    val event2 = channel.take()
    Assert.assertEquals(new String(event2.getBody), "fileline2")
    LOG.info("event body contains " + new String(event2.getBody))

    txn.commit()
    txn.close()
    Thread.sleep(1000)
    Assert.assertEquals(source.sourceVFScounter.getEventCount, 2)
    Assert.assertEquals(source.sourceVFScounter.getFilesCount, 1)
  }

//  /**
//    * If timeout is exceeded, the file must be processed.
//    */
//  @Test
//  def testlastModifiedTimeExceededTimeoutIsTrue() = {
//    val context = new Context()
//    context.put("work.dir", tmpDir.toString)
//    context.put("process.discovered.files", "false")
//    context.put("timeout.start.process", "60")
//    Configurables.configure(source, context)
//    source.start()
//
//
//    val file: Path = Files.createTempFile(tmpDir, "file1", ".txt")
//    Files.write(file, "fileline1\nfileline2\n".getBytes(), StandardOpenOption.APPEND)
//    LOG.info("create file " + file + " lastModifiedTime is " + Files.getLastModifiedTime(file))
//    val lastModifiedTime: Long = Files.getLastModifiedTime(file).toMillis
//
//    Files.setLastModifiedTime(file, FileTime.fromMillis(lastModifiedTime - 120000))
//    LOG.info("modifiying lastmodifiedtime attribute to " + Files.getLastModifiedTime(file))
//
//    val timeout = context.getString("timeout.start.process").toInt
//    val cal = Calendar.getInstance
//    cal.setTime(new Date)
//    cal.add(Calendar.SECOND, -timeout)
//    val timeoutAgo: Date = cal.getTime
//    val dateModified = new Date(Files.getLastModifiedTime(file).toMillis)
//    Assert.assertTrue(dateModified.compareTo(timeoutAgo) < 0)
//    assertThat(java.lang.Long.valueOf(Files.getLastModifiedTime(file).toMillis),lessThan(java.lang.Long.valueOf(timeoutAgo.getTime)))
//
//    Thread.sleep(10000)
//
//    val txn: Transaction = channel.getTransaction
//    txn.begin()
//
//    (1 to 2).toList.foreach(i => {
//      val event = channel.take()
//      Assert.assertEquals(new String(event.getBody), "fileline" + i)
//      LOG.info("event " + i + " body contains " + new String(event.getBody))
//    })
//
//    txn.commit()
//    txn.close()
//
//    Thread.sleep(1000)
//    Assert.assertEquals(source.sourceVFScounter.getEventCount, 2)
//    Assert.assertEquals(source.sourceVFScounter.getFilesCount, 1)
//  }

  /**
    * If timeout is still before to lasModificationtime of the file, do not yet process.
    */
  @Test
  def testlastModifiedTimeExceededTimeoutIsFalse() = {
    val context = new Context()
    context.put("work.dir", tmpDir.toString)
    context.put("process.discovered.files", "false")
    context.put("timeout.start.process", "60")
    Configurables.configure(source, context)
    source.start()


    val file: Path = Files.createTempFile(tmpDir, "file1", ".txt")
    Files.write(file, "fileline1\nfileline2\n".getBytes(), StandardOpenOption.APPEND)
    LOG.info("create file " + file + " lastModifiedTime is " + Files.getLastModifiedTime(file))
    val lastModifiedTime: Long = Files.getLastModifiedTime(file).toMillis

    Files.setLastModifiedTime(file, FileTime.fromMillis(lastModifiedTime - 30000))
    LOG.info("modifiying lastmodifiedtime attribute to " + Files.getLastModifiedTime(file))

    val timeout = context.getString("timeout.start.process").toInt
    val cal = Calendar.getInstance
    cal.setTime(new Date)
    cal.add(Calendar.SECOND, - timeout)
    val timeoutAgo: Date = cal.getTime
    val dateModified = new Date(Files.getLastModifiedTime(file).toMillis)
    Assert.assertTrue(dateModified.compareTo(timeoutAgo) > 0)
    assertThat(java.lang.Long.valueOf(Files.getLastModifiedTime(file).toMillis),greaterThan(java.lang.Long.valueOf(timeoutAgo.getTime)))

    val txn: Transaction = channel.getTransaction
    txn.begin()

    (1 to 2).toList.foreach(i => {
      val event = channel.take()
      Assert.assertNull(event)
    })

    txn.commit()
    txn.close()

    Thread.sleep(1000)
    Assert.assertEquals(source.sourceVFScounter.getEventCount, 0)
    Assert.assertEquals(source.sourceVFScounter.getFilesCount, 0)
  }

}
