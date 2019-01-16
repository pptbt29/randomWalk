package au.csiro.data61.randomwalk.tool

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession

class HDFSWriter(val spark: SparkSession, path: String, overwrite: Boolean) {

  val conf: Configuration = spark.sparkContext.hadoopConfiguration
  val fs: FileSystem = FileSystem.get(conf)
  val writer: FSDataOutputStream = fs.create(new Path(path), overwrite)

  def write(data:String) = {
    writer.write(data.getBytes)
  }
  def writeLine(data: String): Unit = {
    writer.write(s"$data\n".getBytes("utf-8"))
  }

  def writeLine(data: Int): Unit = {
    writeLine(data.toString)
  }

  def writeLine(data: Double): Unit = {
    writeLine(data.toString)
  }

  def flush(): Unit = {
    writer.hflush()
  }

  def newLine(): Unit = {
    writeLine("\n")
  }

  def close(): Unit = {
    writer.close()
  }

}
