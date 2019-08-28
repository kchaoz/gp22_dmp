package dmp.sparkcore

import dmp.Utils.{PersistUtils, RqtUtils}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object MediaAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.parquet.compression.codec", "snappy")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val fileDF: DataFrame = spark.read.parquet("data/in/in-20190820-01")
    val appidAndAppnameRDD = getAppidAndAppnameRDD(spark, "data/app_dict.txt")

    method(spark, fileDF, appidAndAppnameRDD)

    spark.stop()
  }

  def method(spark: SparkSession, df: DataFrame, appidAndAppnameRDD: RDD[(String, String)]) = {
    // 广播变量 appidAndAppnameRDD 的值
    val broadcastIPAndName: Broadcast[Array[(String, String)]] = spark.sparkContext
      .broadcast(appidAndAppnameRDD.collect())

    // 获取 logsAppInfoRDD
    val logsAppInfoRDD: RDD[(String, (String, List[Double]))] = df.rdd.map(row => {
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

      // 获取 key
      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")

      // 创建三个对应的方法处理九个指标
      val requestList = RqtUtils.request(requestmode, processnode)
      val clickList = RqtUtils.click(requestmode, iseffective)
      // iseffective: Int, isbilling: Int, isbid: Int, iswin: Int, adorderid: Int,
      // winprice: Double, adpayment: Double
      val adList = RqtUtils.ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      (appid, (appname, requestList ++ clickList ++ adList))
    })

    // 获取 dictAppInfoRDD
    val dictAppInfoRDD: RDD[(String, String)] = spark.sparkContext.parallelize(broadcastIPAndName.value)

    // 获取joinRDD: logsRDD leftOuterJoin dictRDD
    val joinRDD: RDD[(String, ((String, List[Double]), Option[String]))] =
      logsAppInfoRDD.leftOuterJoin(dictAppInfoRDD)

    // 处理 name（key）
    val result: RDD[(String, List[Double])] = joinRDD.map(tup => {
      if (tup._2._1._1.equals("其他") && tup._2._2.getOrElse(null) != null) {
        (tup._2._2.getOrElse(null), tup._2._1._2)
      } else if (tup._2._1._1.equals("其他") && tup._2._2.getOrElse(null) == null) {
        ("dict中不存在对应名称", tup._2._1._2)
      } else {
        (tup._2._1._1, tup._2._1._2)
      }
    })

    // 结果展示
    def showResult(result: RDD[(String, List[Double])]) = {
      result
//        .filter(_._1.equals("dict中不存在对应名称"))
        .reduceByKey((lst, lst2) => (lst zip lst2).map(tup => tup._1 + tup._2))
        .map(t => t._1 + ", " + t._2.mkString(","))
        .foreach(println)
    }

    showResult(result)

    // 存储到 mysql
    def res2Mysql(result: RDD[(String, List[Double])]) = {
      val res2Mysql = result.reduceByKey((lst, lst2) => (lst zip lst2).map(tup => tup._1 + tup._2))
      res2Mysql.foreachPartition(PersistUtils.rdd2Mysql)
    }

    //    res2Mysql(result)
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
