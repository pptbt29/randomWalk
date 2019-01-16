package au.csiro.data61.randomwalk.tool

import java.io.{BufferedInputStream, ByteArrayOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession

class HDFSReader(val spark: SparkSession, path: String) {

  val conf: Configuration = spark.sparkContext.hadoopConfiguration
  val fs: FileSystem = FileSystem.get(conf)
  val reader: FSDataInputStream = fs.open(new Path(path))


  def readAll(): String = {
    val bis = new BufferedInputStream(reader)
    val buf = new ByteArrayOutputStream()
    var result = bis.read()
    while (result != -1) {
      buf.write(result)
      result = bis.read()
    }
    buf.toString("UTF-8")

  }


  def close(): Unit = {
    reader.close()
  }

}
