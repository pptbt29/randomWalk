package au.csiro.data61.randomwalk.tool

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.{mutable => m}

object PhoneNumberPairTool {
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  import spark.implicits._

  var phoneNumber2vec: DataFrame = _

  case class PhoneNumberConsineSimilarityPair(phoneNumber: String, cosSim: Double)

  /**
    * Read the phoneNumber2vec file and generate the phoneNumber2vec dataframe
    * the format of data from the input file should be
    * phoneNumber vector[0] vector[1] vector[2] ... vector[n]
    *
    * @param phoneNumber2vecFilePath the path of input file
    * @return
    */
  def setPhoneNumber2vec(phoneNumber2vecFilePath: String): Unit = {
    phoneNumber2vec = spark.sparkContext.textFile(phoneNumber2vecFilePath)
      .map((line: String) => {
        val arrayTemp: Array[String] = line.split("\\s")
        (arrayTemp(0), arrayTemp.slice(1, arrayTemp.length).map(_.toDouble))
      }).toDF("number", "vector")
  }

  def getVectorOfPhoneNumber(phoneNumber: String): Array[Double] = {
    val rowOfPhoneNumber2vec = phoneNumber2vec.where($"number" === phoneNumber)
      .collect()
    if (rowOfPhoneNumber2vec.isEmpty) throw new Exception(s"$phoneNumber cannot be found in number2vec")
    else rowOfPhoneNumber2vec(0).getAs[m.WrappedArray[Double]]("vector").toArray
  }

  def calculateCosineSimilarity(phoneNumber1: String, phoneNumber2: String): Double = {
    val vector1: Array[Double] = getVectorOfPhoneNumber(phoneNumber1)
    val vector2: Array[Double] = getVectorOfPhoneNumber(phoneNumber2)
    CosineSimilarity.cosineSimilarity(vector1, vector2)
  }

  def getTopNSimilarity(phoneNumber: String, n: Int): Array[String] = {
    val vectorOfPhoneNumberBc = spark.sparkContext.broadcast(getVectorOfPhoneNumber(phoneNumber))
    phoneNumber2vec.rdd.map { case Row(phoneNumber: String, vector: m.WrappedArray[Double]) =>
      val vectorOfPhoneNumber = vectorOfPhoneNumberBc.value
      val cosSim: Double = CosineSimilarity.cosineSimilarity(vectorOfPhoneNumber, vector.toArray)
      (cosSim, (phoneNumber, vector.toArray))
    }.sortByKey(ascending = false)
      .map { case (cosSim, (phoneNumber: String, vector: Array[Double])) =>
        print(s"phone number: $phoneNumber  cos similarity: $cosSim  vector: [ ")
        vector.foreach(vectorElement => print(s"$vectorElement "))
        println("]")
        phoneNumber
      }.top(n)
  }
}
