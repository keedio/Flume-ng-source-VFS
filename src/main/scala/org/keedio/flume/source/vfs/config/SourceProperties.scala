package org.keedio.flume.source.vfs.config

import java.nio.file.Paths

/**
  * Created by luislazaro on 18/4/18.
  * lalazaro@keedio.com
  * Keedio
  */
object SourceProperties {

  val WORKING_DIRECTORY = "work.dir"
  val DEFAULT_WORKING_DIRECTORY = ""

  val OUTPUT_DIRECTORY = "processed.dir"
  val DEFAULT_OUTPUT_DIRECTORY = System.getProperty("java.io.tmpdir")

  val PATTERN_FILES_MATCH = "includePattern"
  val DEFAULT_PATTERN_FILES_MATCH = """[^.]*\.*?"""

  val PROCESS_FILE_DISCOVERED = "process.discovered.files"
  val DEFAULT_PROCESS_FILE_DISCOVERED = true

  val TIMEOUT_PROCESS_FILES = "timeout.start.process"
  val DEFAULT_TIMEOUT_PROCESS_FILES = 0

  val ACTION_TO_TAKE_WHEN_PROCESS_FILES = "post.process.file"
  val DEFAULT_ACTION_TO_TAKE_WHEN_PROCESS_FILES = ""

  val PATH_TO_STATUS_FILE = "status.file.dir"
  val DEFAULT_PATH_TO_STATUS_FILE: String = Paths.get(System.getProperty("java.io.tmpdir")).toString

  val RECURSIVE_DIRECTORY_SEARCH = "recursive.directory.search"
  val DEFAULT_RECURSIVE_DIRECTORY_SEARCH = true

  val RETAIN_DELETED_FILES_IN_MAP_WHEN_POSTPROCESS = "keep.deleted.files.in.map"
  val DEFAULT_RETAIN_DELETED_FILES_IN_MAP_WHEN_POSTPROCESS = false

  val DELAY_BETWEEN_FILEMONITOR_RUNS = "delay.between.runs"
  val DEFAULT_DELAY_BETWEEN_FILEMONITOR_RUNS = 10

  val MAX_FILES_CHECK_PER_RUN = "files.check.per.run"
  val DEFAULT_FILES_CHECK_PER_RUN = 1000






}
