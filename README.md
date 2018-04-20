# Flume-ng-source-VFS

## Main goal
**Flume-ng-source-VFS** is a custom Apache Flume source component for processing files under supported file sytems by Apache-Commons-Vfs2™.

## Description
Files created or modified will be discovered and sent to flume to be processed by lines.

## Supported File Systems
Apache Commons VFS supports [multiple file systems](https://commons.apache.org/proper/commons-vfs/filesystems.html), however Flume-ng-source-vfs has only been tested in the following one:

* **File**: `file:///home/someuser/somedir`
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
````
# www.keedio.com

# example configuration for VFS sources.
# A single Agent with three sources, two local to filesystem and a third one pointing to remote FTP.

# local1: process files in local directory called incoming_1. Files will be processed when 30 seconds
#         have elapsed since the atributte lastmodifiedtime of the file has changed. Files to be processed must have
#         extension "txt". If flume starts and incoming_1 already contains files, do not process them (discovered).
#         When finish processing , move file to 'processed.dir'. Only source local1 will keep satus file for tracking
#         processed files under path /home/flume/status_local1. The name of the file is local1.ser  (<sourcename>.ser)
#         The other source will keep its status file under temporal folder (default)





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
agens.our.ces.local1.status.file.dir = /home/flume/status_local1



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
````


4. **Move config file to conf directory**

     ```
     $ cp flume-ng-source-vfs.conf  apache-flume-1.8.0-bin/conf/
     ```

5. **Launch flume binary:**

     ```
    $ ./bin/flume-ng agent -c conf -conf-file conf/flume-ng-source-vfs.conf --name agent -Dflume.root.logger=INFO,console
     ```

### Configurable parameters

|Parameter|Description|mandatory|default|observations|
|------|-----------|---|----|---|
|work.dir|path for incoming files|yes|-|/home/flume/incoming|
|includePattern| [Java Regular Expression](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html) for matching files' names|no|everything|"\\\\.*.txt" just txt files|
|processed.dir|if property set, files processed will be moved to dir (path for out files)|no|-|/home/flume/out, remember check for permissions
|process.discovered.files|process files that were in incoming before launching|no|true|true or false|
|timeout.start.process|Process file if 'timeout' seconds have passed since the last modification of the file. Intended for huge files being downloaded to incoming with high network latency. |no|- |For example 60 (seconds), The timeout set by this property is recalculated basis on 'getFileSystem.getLastModTimeAccuracy'|
|post.process.file|If file is successfully processed by source, move or delete|no|-|move or delete. Rememeber to check for permissions. If move files is set but target directory does not exists, file will not be moved.|
|status.file.dir|Directory where a status file called \<sourcename>.ser will be created for keeping track of processed files.|no|temporal folder|The serialized information is a simple map of filename vs size |

## Notes on usage.
+ When scanning for files in 'work.dir', files in subdirectories will also be cached and processed. At the moment (0.2.0), it is not configurable.
+ In some use cases, files to be processed by flume are not yet completed (full content) while downloading to incoming, i.e., the file have already started being processing and at the same moment new lines are being appended. Flume-vfs treats this lines like modifications over a file already cached, processing them in normal way. If network latency is high it can cause issues like truncated data, even with small files. For this cases use parameter timeout.start.process.
+ If a file haven been correctly processed, it's name and size are tracked in an external file than is reloaded when flume is restarted. This file is saved on temporal directory. Actually it is not configurable. If flume stops and a file is not yet finished processing, the file will be processed again since start, producing repeated messages.

## Notes on supported and tested file systems ##

| Scheme | Observations |
| ------ | ------ |
|  ftp  |   In most cases the ftp client will be behind a FW so Passive Mode is set to true by default. Active mode is not working in actual version. Anyway, if you need explicitly Active mode just set in source code `setPassiveMode(options, false)`.|

## Wiki
 [Documentation Flume-ng-source-VFS](https://github.com/keedio/Flume-ng-source-VFS/wiki)

### Version history #####
- 0.2.2
    + Recursive search directory is configurable. (Check out for more information in wiki [Issues found](https://github.com/keedio/Flume-ng-source-VFS/wiki/NOTES#issues-found)
    + Directory for keeping track of processed files is configurable.

- 0.2.1
    + New configurable parameter for setting an action to take when file has been successfully processed. Move or delete.
- 0.2.0
    + Moving files after being processed is done by VFS2 instead of FileUtils.
    + New configurable parameter to delay the beginning of file processing.
- 0.1.0 First stable release
* * *