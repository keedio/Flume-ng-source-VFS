# Flume-ng-source-VFS

## Main goal
**Flume-ng-source-VFS** is a custom Apache Flume source component for processing files under supported file sytems by Apache-Commons-Vfs2™.

## Description
Files created or modified will be discovered and sent to flume to be processed by lines.

## Supported File Systems
Apache Commons VFS supports [multiple file systems](https://commons.apache.org/proper/commons-vfs/filesystems.html), however Flume-ng-source-vfs has only been tested in the following one:

* **File**: `file:///home/someuser/somedir`  , `C:\\flume_incoming`
* **FTP**:  `ftp://myusername:mypassword@somehost/somedir`

## Compilation and packaging
1.**Clone the project:**
```
git clone https://github.com/keedio/flume-ng-source-vfs.git
```

2.**Build with Maven:**
```
mvn clean package
```

### Deployment and launching ###

1. **[Create plugins.d directory](https://flume.apache.org/FlumeUserGuide.html#the-plugins-d-directory).**
2. **[Directory layout for plugins](https://flume.apache.org/FlumeUserGuide.html#directory-layout-for-plugins):**

   ```
    $ cd plugins.d
    $ mkdir flume-source-vfs
    $ cd flume-source-vfs
    $ mkdir lib
    $ cp flume-source-vfs.jar /lib
  ```


3. **Create a config file, agent example**

```
# www.keedio.com

# example configuration for VFS sources.
# A single Agent with three sources, two local to filesystem and a third one pointing to remote FTP.

# local1: process files in local directory called incoming_1. Files will be processed when 30 seconds
#         have elapsed since the atributte lastmodifiedtime of the file has changed. Files to be processed must have
#         extension "txt". If flume starts and incoming_1 already contains files, do not process them (discovered).
#         When finish processing , move file to 'processed.dir'. Only source local1 will keep satus file for tracking
#         processed files under path /home/flume/status_local1. The name of the file is local1.ser  (<sourcename>.ser)
#         The other sources will keep its status file under temporal folder (default)





#ACTIVE LIST
agent.sources =  local1 local2 ftp1


## A source called local1 is retrieving files from local filesystem

agent.sources.local1.type = org.keedio.flume.source.vfs.source.SourceVFS
agent.sources.local1.work.dir = /home/flume/incoming_1
agent.sources.local1.includePattern = \\.*.txt
agent.sources.local1.processed.dir = /home/flume/processed
agent.sources.local1.process.discovered.files = false
agent.sources.local1.timeout.start.process = 30
agent.sources.local1.post.process.file = move
agent.sources.local1.status.file.dir = /home/flume/status_local1



## A source called local2 is retrieving files from local filesystem

agent.sources.local2.type = org.keedio.flume.source.vfs.source.SourceVFS
agent.sources.local2.work.dir = /home/flume/incoming_2
agent.sources.local2.includePattern = \\.*.csv
agent.sources.local2.processed.dir = /home/flume/processed
agent.sources.local2.process.discovered.files = true
agent.sources.local2.post.process.file = delete



## A source called ftp1 is retrieving files from a remote FTP filesystem

agent.sources.ftp1.type = org.keedio.flume.source.vfs.source.SourceVFS
agent.sources.ftp1.work.dir = ftp://user:pass@192.168.0.3/incoming
agent.sources.ftp1.includePattern = \\.*.remote.txt
agent.sources.ftp1.process.discovered.files = false
agent.sources.ftp1.processed.dir = ftp://user:pass@192.168.0.3/out
agent.sources.ftp1.post.process.file = move

##end of sources configuration for Agent 'agent'
```


4. **Move config file to conf directory**

     ```
     $ cp flume-ng-source-vfs.conf  apache-flume-1.8.0-bin/conf/
     ```

5. **Launch flume binary:**

     ```
    $ ./bin/flume-ng agent -c conf -conf-file conf/flume-ng-source-vfs.conf --name agent -Dflume.root.logger=INFO,console
     ```

## Configuration

### Basic Configurable parameters
The only required parameter for starting a agent with a Keedio vfs source is "work.dir". You can add a regular expression.

|Parameter|Description|
|------|:-----------|
|**```work.dir```**|path for incoming files, example /incoming|
|**```includePattern```**| [Java Regular Expression](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html) for matching files' names, example "\\\\.*.txt" just txt files|

###  Configurable parameters that control the behavior of file processing.
The following parameters are optional. They control what the source should do when a file is
found and if we want to do something else to the file after have been processed by flume.
It is intended for being useful in a use case where moving or deleting files is needed,
after flume processes lines, but ideally the source should not do this (neither delete nor moving),
because adds overhead.

|Parameter                      |Description|
|------------------------------ |-----------|
|**```post.process.file```**        |If file is successfully processed by source, move or delete. By<br> default do nothing. If move files is set but target directory<br> does not exists, file will not be moved.This property adds overhead<br> and reduces performance. If the associated property "timeout.start.post.process"<br> is not set with a reasonable amount of seconds it can provoke loosing events.<br> Check for 'timeout.start.post.process'|   |
|**```processed.dir```**|If property set, files processed will be moved to dir,<br> example /home/flume/out, remember check for permissions.|
|**```timeout.start.post.process```**|Post-process files (delete or move) if 'timeout' seconds have passed<br> since the last modification of the file. The file's attribute Last modified time will<br> be checked and if exceeds the threshold (timeout)<br> files will be deleted. If file is still been processed the delay will be increased <br> in another x seconds. Check for more information on Notes os usage. <br><br>***Be careful with this property. If the last modification of the file happens<br> later than the configured timeout, the event will be lost because the file<br> was deleted or moved by exceeding the threshold that determines <br> whether it could be erased or not***.
|**```process.discovered.files```**|Upon starting agent, there were already files. <br> Read on startup agent, default is true|
|**```timeout.start.process```**              |Process file if 'timeout' seconds have passed since the<br> last modification of the file. Intended for huge files<br> being downloaded to incoming with high network latency.<br>For example 60 (seconds), The timeout set by this property<br> is recalculated basis on 'getFileSystem.getLastModTimeAccuracy'|
|**```recursive.directory.search```**|descend in flume's incoming subdirectories for processing files,<br> default is true. Check [Wiki](https://github.com/keedio/Flume-ng-source-VFS/wiki/NOTES#april-20-2018)|


### Configurable parameters for the tracking of processed files.
When a file is processed or at least a chunk of it, name, lines and size are stored in a map. Such a map is written to filesystem.

|Parameter|Description|
|------|-----------|
|**```status.file.dir```**|Directory where a status file called \'\<sourcename>.ser\' will be created` for <br>keeping track of processed files. Default is temporal<br> folder.The serialized information is a simple<br> map of filename vs size |
|**```save.processed.files.onStop```**|When stopping source, status file is written to disk.'true or false'. Default is true|
|**```save.processed.files.schedule```**|Schedule a task for writing status file to disk.'true or false'. Default is true,<br> false will not save data, except on stopping agent <br> (if property save.processed.files.onStop is false)|
|**```time.interval.save.status```**| Interval of time between writes (flushes) from map to file ".ser".<br> Default is 3600 seconds (1 hour), and always when stopping source.<br> The smaller the number, the worse the performance.   |
|**```max.count.map.files```**|When starting agent or reloading by config, the file 'sourcename.ser' with<br> map is loaded in memory.<br> This parameter sets a limit for number of files in map before loading. If total files in<br> map is upper than the parameter, a purge of the oldest files is triggered,<br> i.e. files which 'last modified time' attribute<br> is older than parameter 'timeout.file.old' will be deleted from<br> map. For example, default is 10000 files and one day timeout. So if reached limit,<br> delete from map yesterday processed files, supposing that agent was restarted today.|
|**```timeout.file.old```**|with 'max.count.map.files', sets a limit for file to be enough old in time to be deleted from map.|
|**```keep.deleted.files.in.map```**|When file has been processed it can be deleted or moved. In such<br> a case the default behavior is do not <br> remove the file's name from the map. Default is<br> true. If false, a file processed and deleted will be reprocessed.<br> In most cases we don't want to process a file already processed<br> (default behavior). For a rotating file in time (same file's name but<br> different content) can be useful. |



### Advanced Configurable parameters
The following parameters regulate the internal behavior of vfs monitor responsible for triggering events.


|Parameter|Description|
|------|-----------|
|**```delay.between.runs```**|The DefaultFileMonitor by Apache VFS is a Thread based polling file system monitor with a 1<br> second delay. We increased it to 10 seconds by default. It is and <br>advanced parameter use carefully. If processing losses events<br> (lines) for huge amount of files, or huge files, increasing this parameter should help.<br> The default is a delay of 10 second for every 1000 files processed|
|**```files.check.per.run```**|Set the number of files to check per run, default is 1000 files.This parameter can be useful<br> if we know in advance the number of files to be processed, and this amount is constant.<br> Combined with a proper 'delay.between.runs', performance can improve substantially,<br> but it does not work miracles. |


## Notes on usage.
+ In some use cases, files to be processed by flume are not yet completed (full content) while downloading to incoming, i.e., the file have already started being processing and at the same moment new lines are being appended. Flume-vfs treats this lines like modifications over a file already cached, processing them in normal way. If network latency is high it can cause issues like truncated data, even with small files. For this cases use parameter timeout.start.process.
+ If a file haven been correctly processed, it's name and size are tracked in an external file than is reloaded when flume is restarted. This file is saved on temporal directory. If flume stops and a file is not yet finished processing, the file will be processed again since start, producing repeated messages.
+ When a file has been processed by flume, by default file will remain in directory "incoming", unless an action to take has been set via property 'post.process.file'. In such a case, if file is moved or deleted, the file's name will be removed from the tracking map. If for some reason the same file reappears in flumes's incoming the file will be reprocessed again producing duplicated events. Setting to true 'keep.deleted.files.in.map' will avoid such a use case.
+ To be able to delete or move the files when they have finished processing by flume, when the source starts, it triggers a program that iterates over the files processed, checking if the last modified time is higher than the threshold set in the configurable property.

## Notes on supported and tested file systems ##

| Scheme | Observations |
| ------ | ------ |
|  ftp  |   In most cases the ftp client will be behind a FW so Passive Mode is set to true by default. Active mode is not working in actual version. Anyway, if you need explicitly Active mode just set in source code `setPassiveMode(options, false)`. Actually not configurable via properties.|


## Basic information on events and traceability

|Message|Description|
|-------|-----------|
|INFO Source stest1 received event: entry_create file file1.txt|A new file was created in "incoming flume"|
|INFO started processing new file: file1.txt| process under flume started|
|INFO End processing new file: file1.txt| End of file reached|
|INFO Source stest1 received event: entry_modify file file1.txt| the file has undergone a change, somehow|
|INFO File exists in map of files, previous lines of file are 2 started processing modified file: file1.txt| start processing changes from line two onwards|
|INFO Source stest1 received event: entry_delete file file1.txt| File was deleted|




## Metrics Analysis
Check for [Data table for metrics](https://github.com/keedio/Flume-ng-source-VFS/wiki/METRICS-ANALYSIS)

## Wiki
 [Documentation Flume-ng-source-VFS](https://github.com/keedio/Flume-ng-source-VFS/wiki)

### Version history #####
- 0.4.0
    + Delay between runs for monitor is now configurable.
    + Files to check per run is now configurable.
    + Added Timestamp and counter lines when processing data for better control over file parallel modification.
    + Post processing files is now an asynchronous execution.
    + Configurable interval between flushes data to file.
    + Configurable max limit of files to keep in map when reload agent.
    + Fix bug: SourceCountersVFS not working properly.

- 0.3.0
    + Recursive search directory is configurable. (Check out for more information in wiki [Issues found](https://github.com/keedio/Flume-ng-source-VFS/wiki/NOTES#issues-found) )
    + Directory for keeping track of processed files is configurable.
    + Keep deleted files in map is configurable.
    + Several improvements and fix minor bugs. [+ info](https://github.com/keedio/Flume-ng-source-VFS/wiki/NOTES#issues-found)

- 0.2.1
    + New configurable parameter for setting an action to take when file has been successfully processed. Move or delete.
- 0.2.0
    + Moving files after being processed is done by VFS2 instead of FileUtils.
    + New configurable parameter to delay the beginning of file processing.
- 0.1.0 First stable release
* * *