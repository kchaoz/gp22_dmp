package dmp.sparksql

import java.util

import dmp.Utils.SqlStatementUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField}

object MediaAnalysisSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    myTest(spark)

    spark.stop()
  }

  def myTest(spark: SparkSession) = {
    val fileDF: DataFrame = spark.read.parquet("data/in/in-20190820-01")
    fileDF.createTempView("logs")

    val appidAndAppnameRDD = getAppidAndAppnameRDD(spark, "data/app_dict.txt")
    val appidAndAppnameDF = getAppidAndAppnameDF(spark, appidAndAppnameRDD)
    appidAndAppnameDF.createTempView("appid_appname_info")

    spark.sql(SqlStatementUtils.mdediaAnalysisSql).show(100)
  }

  def getAppidAndAppnameDF(spark: SparkSession, appidAndAppnameRDD: RDD[(String, String)]) = {
    val structFields = new util.ArrayList[StructField]()
    structFields.add(DataTypes.createStructField("appid", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("appname", DataTypes.StringType, false))
    val schema = DataTypes.createStructType(structFields)

    val rowRDD: RDD[Row] = appidAndAppnameRDD.map(tup => Row(tup._1, tup._2))

    spark.createDataFrame(rowRDD, schema)
  }

  def getAppidAndAppnameRDD(spark: SparkSession, path: String) = {
    val fileRDD: RDD[String] = spark.sparkContext.textFile(path) // data/app_dict.txt

    fileRDD.filter(_.split("\t", -1).length >= 5)
      .map(line => {
        val fields = line.split("\t", -1)
        val appid = fields(4)
        val appname = fields(1)
        (appid, appname)
      })
  }

}
