import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.{ListBuffer, Seq}

object PopurlarItemsApp {

  def get_dirnames(days: Int, prefix: String): Seq[String] = {

    val dayList = new ListBuffer[String]()

    for (day <- 0 to days) {
      val date = LocalDateTime.now().minusDays(day);
      val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
      val dirName = prefix + dtf.format(date)
      dayList += dirName
    }

    return dayList
  }

  def get_full_paths(rootPath: String, daysAgo: Int, prefix: String, file:String) : Seq[String] = {
    val dirnames = get_dirnames(daysAgo, prefix)
    val paths = new ListBuffer[String]()

    for (dirname <- dirnames) {
      val abs_path = rootPath + dirname + "/" + file
      paths += abs_path
    }

    return paths
  }

  val usage = """
       --source_bucket : Data root path
       --rdb_endpoint : rdb sink endpoint
       --rdb_database : rdb database name
       --rdb_table : rdb table name
       --rdb_user : rdb user id
       --rdb_pwd : rdb pwd
       --days_ago : data range from today to start day
    """

  def main(args: Array[String]): Unit = {

    if (args.length != 7) {
      println("current args: " + args)
      println(usage)
      return
    }

    var source_bucket: String = ""
    var rdb_endpoint: String = ""
    var rdb_database: String = ""
    var rdb_table: String = ""
    var rdb_user: String = ""
    var rdb_pwd: String = ""
    var days_ago: Int = 0

    args.sliding(2, 2).toList.collect {
      case Array("--source_bucket", arg: String) => source_bucket = arg
      case Array("--rdb_endpoint", arg: String) => rdb_endpoint = arg
      case Array("--rdb_database", arg: String) => rdb_database = arg
      case Array("--rdb_table", arg: String) => rdb_table = arg
      case Array("--rdb_user", arg: String) => rdb_user = arg
      case Array("--rdb_pwd", arg: String) => rdb_pwd = arg
      case Array("--days_ago", arg: String) => days_ago = arg.toInt
    }

    println("**** source_bucket : " + source_bucket)
    println("**** rdb_endpoint : " + rdb_endpoint)
    println("**** rdb_database : " + rdb_database)
    println("**** rdb_table : " + rdb_table)
    println("**** rdb_user : " + rdb_user)
    println("**** rdb_pwd : " + rdb_pwd)
    println("**** days_ago : " + days_ago)


    val paths = get_full_paths(rootPath=source_bucket, daysAgo=days_ago, prefix="yyyymmdd=", file="*.parquet")

    val spark = SparkSession.builder.appName("apmalPopularPrdApp").master("yarn").getOrCreate()

    val rawDF = spark.read.format("parquet").load(paths: _*).select(col("prd"))

    val schema = new StructType()
      .add("prd_cd", StringType)
      .add("prd_nm", StringType)
      .add("prd_norm_prc", StringType)
      .add("prd_brnd_nm", StringType)
      .add("prd_tp_cat_vl", StringType)

    val parsedDF = rawDF.select(col("prd"), from_json(col("prd"), schema).alias("json"))

    val BrandPrdDF = parsedDF.select(trim(col("json")("prd_brnd_nm")).alias("prd_brnd_nm"),
                                     trim(col("json")("prd_nm")).alias("prd_nm"),
                                     trim(col("json")("prd_cd")).alias("prd_cd"))
                              .na.drop()

    val prdCountDF = BrandPrdDF.groupBy(col("prd_brnd_nm"), col("prd_cd"))
                               .agg(count("*").alias("count"))
                               .na.drop()

    val skuDF = prdCountDF.groupBy("prd_brnd_nm")
                          .agg(count("prd_cd").alias("sku_count"))

    val baseDF = prdCountDF.join(skuDF, "prd_brnd_nm").orderBy(desc("count"))

    val overCategory = Window.partitionBy("prd_brnd_nm").orderBy(desc("count"))

    val rankDF = baseDF.withColumn("rank", rank().over(overCategory))
                       .select("prd_brnd_nm", "prd_cd", "count", "rank", "sku_count")
                       .where(col("rank") < 50)

    val jdbc_endpoint = rdb_endpoint + "/" + rdb_database

    rankDF.write.format("jdbc")
      .option("url", jdbc_endpoint)
      .option("dbtable", rdb_table)
      .option("user", rdb_user)
      .option("password", rdb_pwd)
      .option("createTableColumnTypes", "prd_brnd_nm VARCHAR(128), prd_cd VARCHAR(128), count INT, rank INT, sku_count INT")
      .mode("overwrite")
      .save()
  }
}
