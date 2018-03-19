/*
    KEEDIO
*//*
    KEEDIO
*/
package org.keedio.flume.source.vfs.metrics

/**
  *
  * @author Luis Lázaro <lalazaro@keedio.com>
  */
trait SourceCounterVfsMBean {
  /**
    *
    * @return
    */
    def getFilesCount: Long

  /**
    *
    */
  def incrementFilesCount(): Unit

  def getEventCount: Long

  def incrementEventCount(): Unit

  def getEventThroughput(): Long

  def getFileThroughput(): Long

  def getLastEventSent: Long

  def getLastFileSent: Long

  def getLastEventSent_Human: String

  def getLastFileSent_Human: String

  def incrementCountSizeProc(size: Long): Unit

  def getCountSizeProcBytes: Long

  def getCountSizeProcKb: Long

  def getCountSizeProcMb: Double

  def getStarTime: Long

  def getStartTime_Human: String

  def getRunningTime: String

}