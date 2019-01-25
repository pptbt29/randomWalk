package au.csiro.data61.randomwalk.tool

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.{mutable => m}

object PhoneNumberPairTool {
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  var phoneNumber2vec: DataFrame = _

  case class PhoneNumberConsineSimilarityPair (phoneNumber: String, cosSim: Double)

  /**
    * Read the phoneNumber2vec file and generate the phoneNumber2vec dataframe
    * the format of data from the input file should be
    * phoneNumber vector[0] vector[1] vector[2] ... vector[n]
    * @param phoneNumber2vecFilePath the path of input file
    * @return
    */
  def setPhoneNumber2vec(phoneNumber2vecFilePath: String): Unit = {
    phoneNumber2vec = spark.sparkContext.textFile(phoneNumber2vecFilePath)
      .map( (line: String) => {
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

  def getTopTenSimilarity(phoneNumber: String): Array[String] = {
    val topTenPhoneNumbersPq: m.PriorityQueue[PhoneNumberConsineSimilarityPair] =
      new m.PriorityQueue[PhoneNumberConsineSimilarityPair]()(Ordering.by((a: PhoneNumberConsineSimilarityPair) => a.cosSim)).reverse
    val vectorOfPhoneNumber = getVectorOfPhoneNumber(phoneNumber)
    println("pq successfully initiate")
    phoneNumber2vec.collect().foreach{ case Row(phoneNumber: String, vector: m.WrappedArray[Double]) =>
      val cosSim = CosineSimilarity.cosineSimilarity(vectorOfPhoneNumber, vector.toArray)
      if(topTenPhoneNumbersPq.length < 10) topTenPhoneNumbersPq.enqueue( PhoneNumberConsineSimilarityPair(phoneNumber, cosSim))
      else {
        if (cosSim > topTenPhoneNumbersPq.head.cosSim) {
          topTenPhoneNumbersPq.enqueue( PhoneNumberConsineSimilarityPair(phoneNumber, cosSim))
          topTenPhoneNumbersPq.dequeue()
        }
      }
    }
    println("Successfully collect top ten phoneNumber")
    val topTenPhoneNumbersArray = new m.ArrayBuffer[String]()
    topTenPhoneNumbersPq.foreach( (numAndCosSim: PhoneNumberConsineSimilarityPair) => {
      println(s"phone number: ${numAndCosSim.phoneNumber}  cosine similarity: ${numAndCosSim.cosSim}")
      topTenPhoneNumbersArray += numAndCosSim.phoneNumber
    })
    topTenPhoneNumbersArray.toArray[String]
  }
}
