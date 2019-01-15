package au.csiro.data61.randomwalk.dataset

import org.apache.spark.sql.{SparkSession, Row, DataFrame}

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

  var pnpWithinDegreeRange: DataFrame = _

  def setDegreeRange(minOutDegree: Int, maxOutDegree: Int, minInDegree: Int, maxIndegree: Int): Unit = {
    outDegreeForEachPhoneNumWithinRange = getOutDegreeForEachPhoneNumWithinRange(minOutDegree, maxOutDegree)
    inDegreeForEachPhoneNumWithinRange = getInDegreeForEachPhoneNumWithinRange(minInDegree, maxIndegree)
  }

  def checkIfDegreeRangeIsSet(): Unit = {
    if (outDegreeForEachPhoneNumWithinRange == null || inDegreeForEachPhoneNumWithinRange == null)
      throw new Exception("Setup the degree range first")
  }

  def getOutDegreeForEachPhoneNumWithinRange(minOutDegree: Int, maxOutDegree: Int): DataFrame = {
    pnp.rdd
      .map { case Row(src_number: String, _) => (src_number, 1.toLong) }
      .reduceByKey(_ + _)
      .filter{ case (_, number: Long) => number >= minOutDegree && number < maxOutDegree}
      .toDF("phone_number", "out_degree")
  }

  def getInDegreeForEachPhoneNumWithinRange(minInDegree: Int, maxInDegree: Int): DataFrame = {
    pnp.rdd
      .map { case Row(_, dest_number: String) => (dest_number, 1.toLong) }
      .reduceByKey(_ + _)
      .filter{ case (_, number: Long) => number >= minInDegree && number < maxInDegree}
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

  def getPnpWithinDegreeRange():DataFrame = {
    checkIfDegreeRangeIsSet()
    val distinctPhoneNumberWithinDegreeRange = inDegreeForEachPhoneNumWithinRange
      .join(outDegreeForEachPhoneNumWithinRange, outDegreeForEachPhoneNumWithinRange("phone_number") === inDegreeForEachPhoneNumWithinRange("phone_number"))
      .select(outDegreeForEachPhoneNumWithinRange("phone_number"))
    val dfTemp = pnp
      .join(distinctPhoneNumberWithinDegreeRange, pnp("src_number") === distinctPhoneNumberWithinDegreeRange("phone_number"))
      .select("src_number", "dest_number")
    pnpWithinDegreeRange = dfTemp
      .join(distinctPhoneNumberWithinDegreeRange, dfTemp("dest_number") === distinctPhoneNumberWithinDegreeRange("phone_number"))
      .select("src_number", "dest_number")
    pnpWithinDegreeRange
  }

  def distinctPhoneCountWithinDegreeRange: Long = {
    checkIfDegreeRangeIsSet()
    outDegreeForEachPhoneNumWithinRange.select("phone_number")
      .union(inDegreeForEachPhoneNumWithinRange.select("phone_number"))
      .distinct().count()
  }

  def distinctPhonePairCountWithinDegreeRange: Long = {
    checkIfDegreeRangeIsSet()
    getPnpWithinDegreeRange().count()
  }
}
