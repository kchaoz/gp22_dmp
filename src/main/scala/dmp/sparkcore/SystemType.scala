package dmp.sparkcore

import dmp.Utils.RqtUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SystemType {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.parquet.compression.codec", "snappy")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val fileDF: DataFrame = spark.read.parquet("data/in/in-20190820-01")
    method(spark, fileDF)

    spark.stop()
  }

  def method(spark: SparkSession, df: DataFrame) = {
    df.rdd.map(row => {
      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值
      val client = row.getAs[Int]("client")
      // 创建三个对应的方法处理九个指标
      val requestList = RqtUtils.request(requestmode, processnode)
      val clickList = RqtUtils.click(requestmode, iseffective)
      /**
        * iseffective: Int, isbilling: Int, isbid: Int, iswin: Int, adorderid: Int, winprice: Double, adpayment: Double
        */
      val adList = RqtUtils.ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      (client, requestList ++ clickList ++ adList)
    })
      .reduceByKey((lst, lst2) => (lst zip lst2).map(tup => tup._1 + tup._2))
      .map(tup => {
        if (tup._1 == 1) {
          ("android", tup._2)
        } else if (tup._1 == 2) {
          ("ios", tup._2)
        } else {
          ("其他", tup._2)
        }
      })
      .map(t => t._1 + ", " + t._2.mkString(","))
      .foreach(println)
  }
}
