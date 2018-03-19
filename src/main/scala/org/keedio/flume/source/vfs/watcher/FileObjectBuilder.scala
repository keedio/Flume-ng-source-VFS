package org.keedio.flume.source.vfs.watcher

import java.net.URI

import org.apache.commons.vfs2._
import org.apache.commons.vfs2.impl.StandardFileSystemManager
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder
/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@keedio.com
  * Keedio
  */
/**
  * VFS2 supported file systems requiere is some cases to
  * specify config parameters
  */
object FileObjectBuilder {

//  private val fsManager = VFS.getManager
//  private val options: FileSystemOptions = new FileSystemOptions()
//  private val fs = new StandardFileSystemManager
//  fs.setCacheStrategy(CacheStrategy.MANUAL)
//  fs.init()

  /**
    * Get a FileObject for the supported file system.
    * @param uri
    * @return
    */
  def  getFileObject(uri: String): FileObject = {
    val scheme = getScheme(uri)
    scheme match {
      case "ftp" => {
        val fs = new StandardFileSystemManager
        val options: FileSystemOptions = new FileSystemOptions()
        val builder = FtpFileSystemConfigBuilder.getInstance()
        builder.setUserDirIsRoot(options, true)
        builder.setPassiveMode(options, true) //set to true if behind firewall
        fs.init()
        fs.resolveFile(uri, options)
      }
      case _ =>
        val fs = new StandardFileSystemManager
        fs.setCacheStrategy(CacheStrategy.MANUAL)
        fs.init()
        fs.resolveFile(uri)
      }
  }

  /**
    * Get the scheme of an URI.
    * @param uriString
    * @return
    */
  def getScheme(uriString: String): String = {
    val uri = new URI(uriString)
    uri.getScheme
  }

}
