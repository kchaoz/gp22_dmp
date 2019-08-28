package dmp.myTest

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PersistDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val fileDF = spark.read.parquet("data/in/in-20190820-01")
//    persist2Mysql(spark, fileDF)
    getUrlAndProp()

    spark.stop()
  }

  def persist2JSON(spark: SparkSession, fileDF: DataFrame): Unit = {
    // 创建 logs 视图
    fileDF.createTempView("logs")
    val result: DataFrame = spark.sql(sql)

    // 数据存储成 json文件到本地，设置分区数为1
    result.coalesce(1)
      .write
//      .partitionBy("provincename", "cityname")
      .json("data/out/out-20190821-03")
  }

  def persist2Mysql(spark: SparkSession, fileDF: DataFrame): Unit = {
    fileDF.createTempView("logs")
    val result: DataFrame = spark.sql(sql)
    val (url, tableName, prop) = getUrlAndProp()
    result.write.mode(SaveMode.Append).jdbc(url, tableName, prop)
  }


  def getUrlAndProp() = {
    val cfg: Config = ConfigFactory.load()
    val prop: Properties = new Properties()
    prop.put("user", cfg.getString("jdbc.user"))
    prop.put("password", cfg.getString("jdbc.password"))
    val url = cfg.getString("jdbc.url")
    val tableName = cfg.getString("jdbc.tableName")
    (url, tableName, prop)
  }

  val sql = "select count(1) ct, provincename, cityname from logs group by provincename, cityname"
}
