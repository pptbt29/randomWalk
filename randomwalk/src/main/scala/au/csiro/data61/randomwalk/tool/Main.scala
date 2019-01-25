package au.csiro.data61.randomwalk.tool

import java.io.File

import org.apache.spark.sql.SparkSession

class Main {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder
      .appName("lalala")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    PhoneNumberPairTool.setPhoneNumber2vec("syang/output/number2vec")
    PhoneNumberPairTool.getTopTenSimilarity("18826419798")
  }
}
