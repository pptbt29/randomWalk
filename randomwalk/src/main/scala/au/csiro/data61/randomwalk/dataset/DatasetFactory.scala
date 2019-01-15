package au.csiro.data61.randomwalk.dataset

import au.csiro.data61.randomwalk.tool.PhoneNumberFilter.regexCheckAndSlicePhoneNumber
import org.apache.spark.sql.{SparkSession, DataFrame}

object DatasetFactory extends Serializable {
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  // src: mobile_number, dest: phone_number
  def phoneNumberPairsGenerator(
                         i_user_contact_start_date: String,
                         i_user_contact_end_date: String,
                         a_user_table_date: String,
                         idsOfSelectedRegions: Array[String] = null
                         ): DataFrame = {
    val user_contact_df = getUserContactPhoneNumberWithinTimeRange( i_user_contact_start_date, i_user_contact_end_date)
    val user_pn_df = getUserPhoneNumberWithinRegion( a_user_table_date)
    user_contact_df.join(user_pn_df, user_contact_df("user_id") === user_pn_df("id"))
      .where($"mobile_number" =!= $"phone_number")
      .select("mobile_number","phone_number")
      .toDF("src_number", "dest_number")
  }

  def getUserContactPhoneNumberWithinTimeRange(
                       i_user_contact_start_date: String,
                       i_user_contact_end_date: String
                     ): DataFrame = {
    spark.udf.register("regexFilter", (phoneNumber: String) => regexCheckAndSlicePhoneNumber(phoneNumber))
    spark.sql(
      s"""
         |SELECT regexFilter(phone_number) as phone_number, user_id
         |FROM dwd.dwd_tantan_eventlog_user_contact_i_d
         |WHERE (dt between '$i_user_contact_start_date' and '$i_user_contact_end_date')
       """.stripMargin
    ).where("phone_number != ''").distinct()
  }

  def getUserPhoneNumberWithinRegion(
                                     user_table_date: String,
                                     idsOfSelectedRegions: Array[String] = null
                                   ): DataFrame = {

    var regionLimitationSql = ""
    if (idsOfSelectedRegions != null && idsOfSelectedRegions.nonEmpty) {
      val idOfFirstRegion = idsOfSelectedRegions(0)
      var sqlTemp = s"region_ids LIKE '%$idOfFirstRegion%' "
      for (i <- 1 until idsOfSelectedRegions.length - 1) {
        val idOfRegion = idsOfSelectedRegions(i)
        sqlTemp = sqlTemp + s"""OR region_ids LIKE '%$idOfRegion%' """
      }
      regionLimitationSql = s"AND ($sqlTemp)"
    }

    spark.udf.register("regexFilter", (phoneNumber: String) => regexCheckAndSlicePhoneNumber(phoneNumber))

    spark.sql(
      s"""
         |SELECT regexFilter(mobile_number) as mobile_number, id FROM dwd.dwd_putong_yay_users_a_d
         |WHERE dt = '$user_table_date' $regionLimitationSql and mobile_number is not NULL
       """.stripMargin).where("mobile_number != ''")
  }
}
