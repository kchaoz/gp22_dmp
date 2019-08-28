package dmp.sparksql

import dmp.Utils.SqlStatementUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object LocationSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val fileDF: DataFrame = spark.read.parquet("data/in/in-20190820-01")
    fileDF.createTempView("logs")

    spark.sql(SqlStatementUtils.locationSql).show(100)

    spark.stop()
  }

}
