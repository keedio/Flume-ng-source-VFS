package org.keedio.flume.source.vfs.config

import java.nio.file.{Path, Paths}

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

  def getWorkingDirectory: String = workingDirectory

  def getOutPutDirectory: String = outPutDirectory

  def getPatternFilesMatch: Regex = patternFilesMatch.r

  def getProcessFilesDiscovered: Boolean = processFilesDiscovered

  def getTimeoutProcessFiles: Integer = timeoutProcessFiles

  def getActionToTakeAfterProcessingFiles: String = actionToTake

  def getStatusFile: String = {
    val pathTo: Path = Paths.get(statusFilePath)
    val statusFileName: String = sourceName + ".ser"
    Paths.get(pathTo.toString, statusFileName).toString
  }

  def getRecursiveSearchDirectory: Boolean = recursiveSearch

}











