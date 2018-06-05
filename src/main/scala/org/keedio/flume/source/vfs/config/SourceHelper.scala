package org.keedio.flume.source.vfs.config

import java.nio.file.{Path, Paths}
import java.util.{Calendar, Date}

import org.apache.flume.Context
import org.keedio.flume.source.vfs.config.SourceProperties._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex

/**
  * Created by luislazaro on 18/4/18.
  * lalazaro@keedio.com
  * Keedio
  */

class SourceHelper(context: Context, sourceName: String) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[SourceHelper])

  private val workingDirectory = context.getString(WORKING_DIRECTORY, DEFAULT_WORKING_DIRECTORY)
  private val outPutDirectory = context.getString(OUTPUT_DIRECTORY, DEFAULT_OUTPUT_DIRECTORY)
  private val patternFilesMatch = context.getString(PATTERN_FILES_MATCH, DEFAULT_PATTERN_FILES_MATCH)
  private val processFilesDiscovered = context.getBoolean(PROCESS_FILE_DISCOVERED, DEFAULT_PROCESS_FILE_DISCOVERED)
  private val timeoutProcessFiles = context.getInteger(TIMEOUT_PROCESS_FILES, DEFAULT_TIMEOUT_PROCESS_FILES)
  private val actionToTake = context
    .getString(ACTION_TO_TAKE_WHEN_PROCESS_FILES, DEFAULT_ACTION_TO_TAKE_WHEN_PROCESS_FILES)

  private val statusFilePath = context.getString(PATH_TO_STATUS_FILE, DEFAULT_PATH_TO_STATUS_FILE)
  private val recursiveSearch = context.getBoolean(RECURSIVE_DIRECTORY_SEARCH, DEFAULT_RECURSIVE_DIRECTORY_SEARCH)
  private val keepFilesInMap = context.getBoolean(RETAIN_DELETED_FILES_IN_MAP_WHEN_POSTPROCESS, DEFAULT_RETAIN_DELETED_FILES_IN_MAP_WHEN_POSTPROCESS)
  private val delayBetweenRuns = context.getInteger(DELAY_BETWEEN_FILEMONITOR_RUNS, DEFAULT_DELAY_BETWEEN_FILEMONITOR_RUNS)
  private val filesPerRun = context.getInteger(MAX_FILES_CHECK_PER_RUN, DEFAULT_FILES_CHECK_PER_RUN)
  private val timeoutPostProcess = context.getLong(TIMEOUT_POST_PROCESS_FILES, DEFAULT_TIMEOUT_POST_PROCESS_FILES)
  private val initialDelayPostProcess = context.getLong(INITIAL_DELAY_TIMEOUT_POST_PROCESS_FILES, DEFAULT_INITIAL_DELAY_TIMEOUT_POST_PROCESS_FILES)

  def getWorkingDirectory: String = workingDirectory

  def getOutPutDirectory: String = outPutDirectory

  def getPatternFilesMatch: Regex = patternFilesMatch.r

  def isProcessFilesDiscovered: Boolean = processFilesDiscovered

  def getTimeoutProcessFiles: Integer = timeoutProcessFiles

  def getActionToTakeAfterProcessingFiles: String = actionToTake

  def getStatusFile: String = {
    val pathTo: Path = Paths.get(statusFilePath)
    val statusFileName: String = sourceName + ".ser"
    Paths.get(pathTo.toString, statusFileName).toString
  }

  def isRecursiveSearchDirectory: Boolean = recursiveSearch
  def isKeepFilesInMap: Boolean = keepFilesInMap

  def getDelayBetweenRuns = delayBetweenRuns

  def getMaxFilesCheckPerRun = filesPerRun

  def getTimeoutPostProcess = timeoutPostProcess

  def getInitialDelayPostProcess = initialDelayPostProcess

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



}











