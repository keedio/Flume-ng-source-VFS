
package org.keedio.flume.source.vfs.metrics

import java.util.concurrent.atomic.AtomicLong

import org.apache.flume.instrumentation.MonitoredCounterGroup
import org.joda.time.{DateTime, Period}

class SourceCounterVfs(val name: String)
  extends MonitoredCounterGroup(MonitoredCounterGroup.Type.SOURCE, name,
    Seq(
      "files_count",
      "eventCount",
      "start_time",
      "lastEventSent",
      "lastFileSent",
      "eventThroughput",
      "fileThroughput",
      "bytesProcessed",
      "KbProcessed",
      "MbProcessed"): _*)
    with SourceCounterVfsMBean {
  private var files_count = 0L
  private var eventCount = 0L
  private var eventThroughput = 0L
  private var fileThroughput = 0L
  private var start_time = System.currentTimeMillis
  private var lastEventSent = 0L
  private var lastFileSent = 0L
  private var bytesProcessed = 0L
  private var KbProcessed = 0L
  private var MbProcessed: Double = 0
  var ATTRIBUTES = Seq(
    "files_count",
    "eventCount",
    "start_time",
    "lastEventSent",
    "lastFileSent",
    "eventThroughput",
    "fileThroughput",
    "bytesProcessed",
    "KbProcessed",
    "MbProcessed"
  )

  val atomicFilesCount = new AtomicLong(0L)
  /**
    * @return long, number of files discovered
    */
  override def getFilesCount: Long = {
    atomicFilesCount.get
  }

  val atomicEventCount = new AtomicLong(0L)

  override def incrementEventCount(): Unit = {
    lastEventSent = System.currentTimeMillis
    atomicEventCount.getAndIncrement()
    if (lastEventSent - start_time >= 1000) {
      val secondsElapsed = (lastEventSent - start_time) / 1000
      eventThroughput = atomicEventCount.get() / secondsElapsed
    }
  }

  /**
    *
    * @return long
    */
  override def getEventCount: Long = {
    atomicEventCount.get()
  }

  override def incrementFilesCount(): Unit = {
    lastFileSent = System.currentTimeMillis();
    atomicFilesCount.getAndIncrement()
    if (lastFileSent - start_time >= 1000) {
      val secondsElapsed = (lastFileSent - start_time) / 1000
      fileThroughput = atomicFilesCount.get / secondsElapsed
    }
  }

  override def getEventThroughput(): Long = eventThroughput

  override def getLastEventSent: Long = lastEventSent

  override def getLastFileSent: Long = lastFileSent

  override def getStarTime: Long = start_time

  override def getLastEventSent_Human: String = {
    val dateTime = new DateTime(lastEventSent)
    dateTime.toString("YYYY-MM-dd_HH:mm:ss.SSS")
  }

  override def getLastFileSent_Human: String = {
    val dateTime = new DateTime(lastFileSent)
    dateTime.toString("YYYY-MM-dd_HH:mm:ss.SSS")
  }

  override def getStartTime_Human: String = {
    val dateTime = new DateTime(start_time)
    dateTime.toString("YYYY-MM-dd_HH:mm:ss.SSS")
  }

  override def incrementCountSizeProc(size: Long): Unit = bytesProcessed += size

  override def getCountSizeProcBytes: Long = bytesProcessed

  override def getCountSizeProcKb: Long = {
    KbProcessed = getCountSizeProcBytes / 1024
    KbProcessed
  }

  override def getCountSizeProcMb: Double = {
    MbProcessed = getCountSizeProcBytes.toDouble / (1024 * 1024)
    MbProcessed
  }

  override def getRunningTime: String = {
    (getEventCount > 0L) match {
      case true => {
        val period = new Period(new DateTime(start_time), new DateTime())
        new String(period.getDays + " " +
          period.getHours + ":" + period.getMinutes + ":" + period.getSeconds)
      }
      case false => "no events"
    }
  }

}