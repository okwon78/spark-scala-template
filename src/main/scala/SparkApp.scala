import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, count, desc, from_json, trim}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.mutable.{ListBuffer, Seq}

object SparkApp {

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

  val rootPath = "s3://ap-pipe-bdp-cdp-log/db_weblog/database/tb_recommend_raw/"

  def main(args: Array[String]): Unit = {
    val paths = get_full_paths(rootPath=rootPath, daysAgo=20, prefix="yyyymmdd=", file="*.parquet")

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
      trim(col("json")("prd_cd")).alias("prd_cd")).na.drop()

    val prdCountDF = BrandPrdDF.groupBy(col("prd_brnd_nm"), col("prd_cd"))
      .agg(count("*").alias("count")).na.drop()
      .orderBy(desc("count"))

    val skuDF = prdCountDF.groupBy("prd_brnd_nm").agg(count("prd_cd").alias("sku_count"))

    val baseDF = prdCountDF.join(skuDF, "prd_brnd_nm")
      .where((col("count") > 200).or(col("sku_count") < 50))
      .orderBy(desc("sku_count"))

    val sortedBaseDF = baseDF.orderBy(desc("count"))

    val brandPopularPrdDF = sortedBaseDF.groupBy("prd_brnd_nm")
      .agg(collect_list("prd_cd").alias("prd_cds"), collect_list("count").alias("counts"))


    brandPopularPrdDF.show(5)
  }
}
