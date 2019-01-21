package au.csiro.data61.randomwalk.dataset

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class PhoneNumberPairDataset(
                              i_user_contact_start_date: String,
                              i_user_contact_end_date: String,
                              a_user_table_date: String,
                              idsOfSelectedRegions: Array[String] = null
                            ) {
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val pnp: DataFrame = DatasetFactory.phoneNumberPairsGenerator(i_user_contact_start_date, i_user_contact_end_date, a_user_table_date)
  var outDegreeForEachPhoneNumWithinRange: DataFrame = _
  var inDegreeForEachPhoneNumWithinRange: DataFrame = _
  var idOfPhoneNumberWithinRange: DataFrame = _

  var pnpWithinDegreeRange: DataFrame = _
  var indexedPnpWithinDegreeRange: DataFrame = _

  def setDegreeRangez(minOutDegree: Int, maxOutDegree: Int, minInDegree: Int, maxIndegree: Int): PhoneNumberPairDataset = {
    outDegreeForEachPhoneNumWithinRange = getOutDegreeForEachPhoneNumWithinRange(minOutDegree, maxOutDegree)
    inDegreeForEachPhoneNumWithinRange = getInDegreeForEachPhoneNumWithinRange(minInDegree, maxIndegree)
    idOfPhoneNumberWithinRange = inDegreeForEachPhoneNumWithinRange.join(outDegreeForEachPhoneNumWithinRange,
      outDegreeForEachPhoneNumWithinRange("phone_number") === inDegreeForEachPhoneNumWithinRange("phone_number"))
      .select(outDegreeForEachPhoneNumWithinRange("phone_number"))
      .rdd.map { case Row(phone_number: String) => phone_number }
      .zipWithIndex().toDF("phone_number", "id")
    this
  }

  def setPnpWithinDegreeRange(): PhoneNumberPairDataset = {
    checkIfDegreeRangeIsSet()
    val dfTemp = pnp
      .join(idOfPhoneNumberWithinRange, pnp("src_number") === idOfPhoneNumberWithinRange("phone_number"))
      .select("src_number", "dest_number")
    pnpWithinDegreeRange = dfTemp
      .join(idOfPhoneNumberWithinRange, dfTemp("dest_number") === idOfPhoneNumberWithinRange("phone_number"))
      .select("src_number", "dest_number")
    this
  }

  def setIndexedPnpWithinDegreeRange(): PhoneNumberPairDataset = {
    checkIfDegreeRangeIsSet()
    val dfTemp = pnp
      .join(idOfPhoneNumberWithinRange, pnp("src_number") === idOfPhoneNumberWithinRange("phone_number"))
      .select("id", "dest_number").toDF("src_number", "dest_number")
    indexedPnpWithinDegreeRange = dfTemp
      .join(idOfPhoneNumberWithinRange, dfTemp("dest_number") === idOfPhoneNumberWithinRange("phone_number"))
      .select("src_number", "id").toDF("src_number_id", "dest_number_id")
    this
  }

  def numberOfDistinctPhoneWithinDegreeRange: Long = {
    checkIfDegreeRangeIsSet()
    idOfPhoneNumberWithinRange.count()
  }

  def numberOfDistinctPhonePairWithinDegreeRange: Long = {
    checkIfDegreeRangeIsSet()
    checkIfEitherPnpWithinDegreeRangeIsSet()
    if (pnpWithinDegreeRange == null) indexedPnpWithinDegreeRange.count()
    else pnpWithinDegreeRange.count()
  }

  def getOutDegreeForEachPhoneNumWithinRange(minOutDegree: Int, maxOutDegree: Int): DataFrame = {
    pnp.rdd
      .map { case Row(src_number: String, _) => (src_number, 1.toLong) }
      .reduceByKey(_ + _)
      .filter { case (_, number: Long) => number >= minOutDegree && number < maxOutDegree }
      .toDF("phone_number", "out_degree")
  }

  def getInDegreeForEachPhoneNumWithinRange(minInDegree: Int, maxInDegree: Int): DataFrame = {
    pnp.rdd
      .map { case Row(_, dest_number: String) => (dest_number, 1.toLong) }
      .reduceByKey(_ + _)
      .filter { case (_, number: Long) => number >= minInDegree && number < maxInDegree }
      .toDF("phone_number", "in_degree")
  }

  def outDegreeVersusTotalNumber(): DataFrame = {
    checkIfDegreeRangeIsSet()
    outDegreeForEachPhoneNumWithinRange.rdd
      .map { case Row(_, out_degree: Long) => (out_degree, 1.toLong) }
      .reduceByKey(_ + _)
      .toDF("out_degree", "total_num")
  }

  def inDegreeVersusTotalNumber(): DataFrame = {
    checkIfDegreeRangeIsSet()
    inDegreeForEachPhoneNumWithinRange.rdd
      .map { case Row(in_degree: Long, _) => (in_degree, 1.toLong) }
      .reduceByKey(_ + _)
      .toDF("in_degree", "total_num")
  }

  def checkIfDegreeRangeIsSet(): Unit = {
    if (outDegreeForEachPhoneNumWithinRange == null || inDegreeForEachPhoneNumWithinRange == null)
      throw new Exception("Setup the degree range first")
  }

  def checkIfEitherPnpWithinDegreeRangeIsSet(): Unit = {
    if (indexedPnpWithinDegreeRange == null && pnpWithinDegreeRange == null)
      throw new Exception("Set indexedPnpWithinDegreeRange or pnpWithinDegreeRange first")
  }
}