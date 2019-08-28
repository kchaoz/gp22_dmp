package dmp.sparkcore

import dmp.Utils.RqtUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object LocationRqt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.parquet.compression.codec","snappy")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val fileDF: DataFrame = spark.read.parquet("data/in/in-20190820-01")
    method(spark, fileDF)

    spark.stop()
  }

  def method(spark: SparkSession, df: DataFrame) = {
    df.rdd.map(row=>{
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
      // key 值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      // 创建三个对应的方法处理九个指标
      val requestList = RqtUtils.request(requestmode, processnode)
      val clickList = RqtUtils.click(requestmode, iseffective)
      /**
        * iseffective: Int, isbilling: Int, isbid: Int, iswin: Int, adorderid: Int, winprice: Double, adpayment: Double
        */
      val adList = RqtUtils.ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      ((pro,city), requestList ++ clickList ++ adList)
    })
      .reduceByKey((lst, lst2) => (lst zip lst2).map(tup => tup._1 + tup._2))
      .map(t => t._1 + ", " + t._2.mkString(","))
      .foreach(println)
//      .saveAsTextFile("data/out/out-20190821-01")

    // 作业
    // 如果存入mysql的话，你需要使用foreachPartition
    // 需要自己写一个连接池
  }

}
