package org.keedio.flume.source.vfs.util

import java.util.{Calendar, Date}

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by luislazaro on 5/6/18.
  * lalazaro@keedio.com
  * Keedio
  */
object SourceHelper {

  val LOG: Logger = LoggerFactory.getLogger(getClass)

  /**
    * Determine whether the attribute 'lastModifiedTime' exceeded argument threshold(timeout).
    * If 'timeout' seconds have passed since the last modification of the file, file can be discovered
    * and processed.
    * If Datemodified is before than DateTimeout we can process, return true
    *
    * @param lastModifiedTime
    * @param timeout ,         configurable by user via property processInUseTimeout (seconds)
    * @return
    */
  def lastModifiedTimeExceededTimeout(lastModifiedTime: Long, timeout: Int): Boolean = {
    val dateModified = new Date(lastModifiedTime)
    val cal = Calendar.getInstance
    cal.setTime(new Date)
    cal.add(Calendar.SECOND, -timeout)
    val timeoutAgo = cal.getTime
    dateModified.compareTo(timeoutAgo) > 0
  }


  /**
    * Returns the timeout set by user but adjusted with the accuracy of the last modification time provided
    * by the file system.
    *
    * @param lastModifiedTimeAccuracy
    * @param baseTimeOut
    * @return
    */
  def adjustTimeout(lastModifiedTimeAccuracy: java.lang.Double, baseTimeOut: Int): Int = {
    val adjustedTimeout = lastModifiedTimeAccuracy.toInt match {
      case 0 => baseTimeOut
      case x if (x > 0) =>
        (baseTimeOut.toLong - x.toLong).toInt
      case _ => baseTimeOut
    }
    if (LOG.isDebugEnabled) {
      LOG.debug("The accuracy of the last modification time provided by file system is " + lastModifiedTimeAccuracy +
        " ms " + ", computed timeout is " + adjustedTimeout + " seconds")
    }

    adjustedTimeout
  }

}
