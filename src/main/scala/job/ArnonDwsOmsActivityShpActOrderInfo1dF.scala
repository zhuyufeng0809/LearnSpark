package job

import conf.MysqlInstance
import job.ArnonDwsOmsActivityShpActOrderInfo1dF.spark
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object ArnonDwsOmsActivityShpActOrderInfo1dF {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("arnon_dws_oms_activity_shp_act_order_info_1d_f")
    .getOrCreate()

  val targetTable: (String, String) =
    ("arnon", "arnon_dws_oms_activity_shp_act_order_info_1d_f")

  def main(args: Array[String]): Unit = {

    registerSourceTableAsView()
    spark.sql(STEP1).persist(StorageLevel.MEMORY_ONLY)
    sinkToTargetTable(spark.sql(STEP2))

    spark.catalog.clearCache()
    spark.close()
  }

  def registerSourceTableAsView(): Unit = {
    val AMinBound = Option(
      spark.read
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL, MysqlInstance.ADB.getUrl)
        .option("user", MysqlInstance.ADB.username)
        .option("password", MysqlInstance.ADB.password)
        .option(
          JDBCOptions.JDBC_QUERY_STRING,
          "select min(ord_id) as min_bound from arnon.arnon_dwd_oms_lgtics_actual_package_f"
        )
        .load()
        .first()
        .getAs("min_bound")
    )

    val AMaxBound = Option(
      spark.read
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL, MysqlInstance.ADB.getUrl)
        .option("user", MysqlInstance.ADB.username)
        .option("password", MysqlInstance.ADB.password)
        .option(
          JDBCOptions.JDBC_QUERY_STRING,
          "select max(ord_id) as max_bound from arnon.arnon_dwd_oms_lgtics_actual_package_f"
        )
        .load()
        .first()
        .getAs("max_bound")
    )

    val BMinBound = Option(
      spark.read
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL, MysqlInstance.ADB.getUrl)
        .option("user", MysqlInstance.ADB.username)
        .option("password", MysqlInstance.ADB.password)
        .option(
          JDBCOptions.JDBC_QUERY_STRING,
          "select min(source_id) as min_bound from arnon.arnon_dwd_oms_trd_origin_order_f"
        )
        .load()
        .first()
        .getAs("min_bound")
    )

    val BMaxBound = Option(
      spark.read
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL, MysqlInstance.ADB.getUrl)
        .option("user", MysqlInstance.ADB.username)
        .option("password", MysqlInstance.ADB.password)
        .option(
          JDBCOptions.JDBC_QUERY_STRING,
          "select max(source_id) as max_bound from arnon.arnon_dwd_oms_trd_origin_order_f"
        )
        .load()
        .first()
        .getAs("max_bound")
    )

    val CMinBound = Option(
      spark.read
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL, MysqlInstance.ADB.getUrl)
        .option("user", MysqlInstance.ADB.username)
        .option("password", MysqlInstance.ADB.password)
        .option(
          JDBCOptions.JDBC_QUERY_STRING,
          "select min(source_id) as min_bound from arnon.arnon_dwd_oms_trd_order_info_f"
        )
        .load()
        .first()
        .getAs("min_bound")
    )

    val CMaxBound = Option(
      spark.read
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL, MysqlInstance.ADB.getUrl)
        .option("user", MysqlInstance.ADB.username)
        .option("password", MysqlInstance.ADB.password)
        .option(
          JDBCOptions.JDBC_QUERY_STRING,
          "select max(source_id) as max_bound from arnon.arnon_dwd_oms_trd_order_info_f"
        )
        .load()
        .first()
        .getAs("max_bound")
    )

    spark.read
      .option("partitionColumn", "ord_id")
      .option("lowerBound", AMinBound.getOrElse(0).toString)
      .option("upperBound", AMaxBound.getOrElse(0).toString)
      .option("numPartitions", 100)
      .jdbc(
        MysqlInstance.ADB.getUrl,
        "arnon.arnon_dwd_oms_lgtics_actual_package_f",
        MysqlInstance.ADB.getProperties
      )
      .createOrReplaceTempView("arnon_dwd_oms_lgtics_actual_package_f")

    spark.read
      .option("partitionColumn", "source_id")
      .option("lowerBound", BMinBound.getOrElse(0).toString)
      .option("upperBound", BMaxBound.getOrElse(0).toString)
      .option("numPartitions", 100)
      .jdbc(
        MysqlInstance.ADB.getUrl,
        "arnon.arnon_dwd_oms_trd_origin_order_f",
        MysqlInstance.ADB.getProperties
      )
      .createOrReplaceTempView("arnon_dwd_oms_trd_origin_order_f")

    spark.read
      .option("partitionColumn", "source_id")
      .option("lowerBound", CMinBound.getOrElse(0).toString)
      .option("upperBound", CMaxBound.getOrElse(0).toString)
      .option("numPartitions", 100)
      .jdbc(
        MysqlInstance.ADB.getUrl,
        "arnon.arnon_dwd_oms_trd_order_info_f",
        MysqlInstance.ADB.getProperties
      )
      .createOrReplaceTempView("arnon_dwd_oms_trd_order_info_f")

    spark.read
      .jdbc(
        MysqlInstance.ADB.getUrl,
        "arnon.arnon_dim_activity_info",
        MysqlInstance.ADB.getProperties
      )
      .createOrReplaceTempView("arnon_dim_activity_info")

    spark.read
      .jdbc(
        MysqlInstance.ADB.getUrl,
        "arnon.arnon_dim_activity_info",
        MysqlInstance.ADB.getProperties
      )
      .createOrReplaceTempView("arnon_dim_activity_info")

    spark.read
      .jdbc(
        MysqlInstance.ADB.getUrl,
        "arnon.arnon_dim_shop",
        MysqlInstance.ADB.getProperties
      )
      .createOrReplaceTempView("arnon_dim_shop")
  }

  def sinkToTargetTable(result: DataFrame): Unit = {
    result.write
      .mode(SaveMode.Overwrite)
      .option(JDBCOptions.JDBC_TRUNCATE, "true")
      .jdbc(
        MysqlInstance.ADB.getUrl,
        String.join(".", targetTable._1, targetTable._2),
        MysqlInstance.ADB.getProperties
      )
  }

  val STEP1: String =
    """create temp view temp_arnon_dws_oms_activity_shp_act_order_info_1d_f as
      |select ss.party_id,
      |       ss.party_name,
      |       a.activity_name,
      |       a.shop_id,
      |       ss.shop_code,
      |       ss.shop_name,
      |       a.activity_starttime,
      |       a.activity_endtime,
      |       cast(0 as tinyint)                   as is_del,
      |       now()                                as etl_create_time,
      |       now()                                as etl_update_time,
      |       a.source_sys
      |from arnon_dim_activity_info a
      |         inner join arnon_dim_shop ss
      |                    on a.activity_type = 'TC'
      |                        and a.activity_starttime <= now()
      |                        and a.activity_endtime >= now()
      |                        and a.activity_name not like '%测试%'
      |                        and a.shop_id <> 591
      |                        and a.source_sys = 1
      |                        and ss.source_sys = 1
      |                        and ss.source_table = 1
      |                        and ss.is_del = 0
      |                        and a.shop_id = ss.source_id
      |                        and ss.shop_classify not in
      |                            ('cross_border', 'distribution_outer', 'POP_outer', 'maochao_outer', 'other_outer',
      |                             'eka_outer');""".stripMargin

  val STEP2: String =
    """select a.party_id,
      |       a.party_name,
      |       a.activity_name    as actvt_name,
      |       a.shop_id,
      |       a.shop_name,
      |       t.ord_num,
      |       t.waybill_num,
      |       0                  as is_del,
      |       now()              as etl_create_time,
      |       now()              as etl_update_time,
      |       a.source_sys
      |from temp_arnon_dws_oms_activity_shp_act_order_info_1d_f a
      |         left join (select max(ss.party_id)                     as party_id,
      |                           max(ss.party_name)                   as party_name,
      |                           ss.activity_name,
      |                           ss.shop_id,
      |                           max(ss.shop_name)                    as shop_name,
      |                           count(distinct oo.platform_order_id) as ord_num,
      |                           count(distinct wap.lgtics_number)    as waybill_num,
      |                           ss.source_sys
      |                    from temp_arnon_dws_oms_activity_shp_act_order_info_1d_f ss
      |                             left join arnon_dwd_oms_trd_origin_order_f oo
      |                                       on ss.shop_code = oo.shop_id
      |                             inner join arnon_dwd_oms_trd_order_info_f oi
      |                                        on oo.source_id = oi.origin_order_id
      |                                            and oi.source_sys = 1
      |                                            and oi.origin_order_id is not null
      |                                            and oo.source_table = oi.source_table
      |                                            and oo.create_time > '2022-08-11'
      |                                            and oo.source_sys = 1
      |                                            and oo.is_del = 0
      |                                            and oi.is_del = 0
      |                                            and
      |                                           (oi.pay_status <> 'UNPAID' or oi.shipping_status in ('OPERATED', 'RECEIVED'))
      |                                            and oi.order_type = 'SALE'
      |                                            and oi.order_status in ('WAIT_CHECK', 'CHECK_PASS')
      |                                            and (
      |                                                   (oo.final_payment_status not in
      |                                                    ('WAIT_FINAL_PAY', 'PAID_FINAL_PAY', 'FINISH_FINAL_PAY')
      |                                                       and oi.paytime >= ss.activity_starttime
      |                                                       and oi.paytime <= ss.activity_endtime)
      |                                                   or
      |                                                   (oo.final_payment_status in ('PAID_FINAL_PAY', 'FINISH_FINAL_PAY')
      |                                                       and oo.final_payment_time >= ss.activity_starttime
      |                                                       and oo.final_payment_time <= ss.activity_endtime)
      |                                               )
      |                             inner JOIN arnon_dwd_oms_lgtics_actual_package_f wap
      |                                        ON oi.source_id = wap.ord_id
      |                                            and wap.source_table = oi.source_table
      |                                            and wap.source_sys = 1
      |                                            and wap.is_del = 0
      |                    group by ss.activity_name, ss.shop_id, ss.source_sys) t
      |                   on a.activity_name = t.activity_name and a.shop_id = t.shop_id and a.source_sys = t.source_sys;""".stripMargin
}
