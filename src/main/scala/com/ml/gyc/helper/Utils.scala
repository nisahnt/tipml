package com.ml.gyc.helper

import org.apache.hadoop.fs.{Path, FileSystem}

object Utils {

  def deletePath(fs : FileSystem, path: String): Unit = {

    if (fs.exists(new Path(path))) {
      val success = fs.delete(new Path(path), true)
      if (!success)
        throw new scala.Exception("Failed to delete existing path: " + path)
    }
  }
}
