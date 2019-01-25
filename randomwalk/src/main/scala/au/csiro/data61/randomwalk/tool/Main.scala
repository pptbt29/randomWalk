package au.csiro.data61.randomwalk.tool

import org.apache.spark.sql.SparkSession

class Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    PhoneNumberPairTool.setPhoneNumber2vec("syang/output/number2vec")
    PhoneNumberPairTool.getTopTenSimilarity("18826419798")
  }
}
