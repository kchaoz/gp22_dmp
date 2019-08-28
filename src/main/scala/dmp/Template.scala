package dmp

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import dmp.myTest.PersistDemo.sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Template {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.parquet.compression.codec","snappy")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val fileDF: DataFrame = spark.read.parquet("data/in/in-20190820-01")

    spark.stop()
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

}
