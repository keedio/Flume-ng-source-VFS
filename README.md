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
# A single Agent with two sources, one local to filesystem and a second one pointing to remote FTP.



#ACTIVE LIST
agent.sources =  local1 ftp1


## A source called local1 is retrieving files from local filesystem
agent.sources.local1.type = org.keedio.flume.source.vfs.source.SourceVFS
agent.sources.local1.work.dir = /home/flume/incoming
agent.sources.local1.includePattern = \\.*.txt
agent.sources.local1.processed.dir = /home/flume/processed
agent.sources.local1.process.discovered.files = false
agent.sources.local1.timeout.start.process = 30

## A source called ftp1 is retrieving files from a remote FTP filysystem
agent.sources.ftp1.type = org.keedio.flume.source.vfs.source.SourceVFS
agent.sources.ftp1.work.dir = ftp://user:pass@192.168.0.3/incoming
agent.sources.ftp1.includePattern = \\.*.remote.txt
agent.sources.ftp1.process.discovered.files = false
agent.sources.ftp1.processed.dir = ftp://user:pass@192.168.0.3/out

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
|work.dir|path for incoming files|yes|-|-|
|includePattern| [Java Regular Expression](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html) for matching files' names|no|-|-|
|processed.dir|if property set, files processed will be moved to dir (path for out files)|no|not move|remember check for permissions
|process.discovered.files|process files that were in incoming before launching|no|true|-|
|timeout.start.process|Process file if 'timeout' seconds have passed since the last modification of the file. Intended for huge files being downloaded to incoming with high network latency. |no|false|The timeout set by this property is recalculated basis on 'getFileSystem.getLastModTimeAccuracy'|

## Notes on usage.
+ When scanning for files in 'work.dir', files in subdirectories will also be cached and processed. At the moment (0.2.0), it is not configurable.
+ In some use cases, files to be processed by flume are not yet completed (full content) while downloading to incoming, i.e., the file have already started being processing and at the same moment new lines are being appended. Flume-vfs treats this lines like modifications over a file already cached, processing them in normal way. If network latency is high it can cause issues like truncated data, even with small files. For this cases use parameter timeout.start.process.
+ If a file haven been correctly processed, it's name and size are tracked in an external file than is reloaded when flume is restarted. This file is saved on temporal directory. Actually it is not configurable. If flume stops and a file is not yet finished processing, the file will be processed again since start, producing repeated messages.

## Notes on supported and tested file systems ##

| Scheme | Observations |
| ------ | ------ |
|  ftp  |   In most cases the ftp client will be behind a FW so Passive Mode is set to true by default. Active mode is not working in actual version. Anyway, if you need explicitly Active mode just set in source code `setPassiveMode(options, false)`.

## Wiki
 [Documentation Flume-ng-source-VFS](https://github.com/keedio/Flume-ng-source-VFS/wiki)

### Version history #####
- 0.2.0
    + Moving files after being processed is done by VFS2 instead of FileUtils.
    + New configurable parameter to delay the beginning of file processing.
- 0.1.0 First stable release
* * *