package dmp

import dmp.Utils.RedisUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object OtherTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

//    RedisUtils.data2Redis(spark, "data/app_dict.txt")

    spark.stop()
  }
}
