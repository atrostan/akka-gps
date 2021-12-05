package com.preprocessing.aggregation

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.{FileSystem, Path => HDFSPath}

object HDFSUtil {

  def getHDFSfs(hadoopConfDir: String) = {
    val conf = new Configuration()
    conf.addResource(new HDFSPath("file:///" + hadoopConfDir + "/core-site.xml"));
    conf.addResource(new HDFSPath("file:///" + hadoopConfDir + "/hdfs-site.xml"));
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    FileSystem.get(conf)
  }

  def createHDFSDirIfNotExists(path: String, fs: FileSystem) = {
    //==== Create folder if not exists//==== Create folder if not exists
    val workingDir = fs.getWorkingDirectory()
    val newFolderPath = new HDFSPath(path)
    if (!fs.exists(newFolderPath)) { // Create new Directory
      fs.mkdirs(newFolderPath)
      println("Path " + path + " created.")
    }
  }

}


