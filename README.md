# Flume-ng-source-VFS

## Main goal
**Flume-ng-source-VFS** is a custom Apache Flume source component for processing files under supported file sytems by Apache-Commons-Vfs2™.

## Description
Files created or modified will be discovered and processed by tailing lines.

## Supported File Systems
Apache Commons VFS supports the following file systems [Link to File Systems](https://commons.apache.org/proper/commons-vfs/filesystems.html), however Flume-ng-source-vfs has only been tested in the following one:

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

# example configuration for VFS source


#ACTIVE LIST
agent.sources =  local1 ftp1


## example a local file system
agent.sources.local1.type = org.keedio.flume.source.vfs.source.SourceVFS
agent.sources.local1.work.dir = /home/flume/incoming
agent.sources.local1.includePattern = \\.*.txt
agent.sources.local1.processed.dir = /home/flume/processed
agent.sources.local1.process.discovered.files = false


agent.sources.ftp1.type = org.keedio.flume.source.vfs.source.SourceVFS
agent.sources.ftp1.work.dir = ftp://user:pass@192.168.0.1/incoming
agent.sources.ftp1.includePattern = \\.*.remote.txt
agent.sources.ftp1.process.discovered.files = false

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
|includePattern| [Java Regular Expression](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html) for mathing files' names|yes|-|-|
|processed.dir|if property set, files processed will be moved to dir|no|not move|not available for FTP
|process.discovered|process files that were in incoming before launching|no|true|-|

## Notes on supported and tested file systems ##

| Scheme | Observations |
| ------ | ------ |
|  ftp  |   In most cases the ftp client will be behind a FW so Passive Mode is set to true by default. Active mode is not working in actual version. Anyway, if you need explicitly Active mode just set in source code `setPassiveMode(options, false)`.


* * *