package dmp.myTest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object ConfSetDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置压缩方式 使用Snappy方式进行压缩
      .set("spark.sql.parquet.compression.codec", "snappy")
    // 创建执行入口
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //    val sc = new SparkContext(conf)
    //    val sQLContext = new SQLContext(sc)
    // 设置压缩方式 使用Snappy方式进行压缩
    //    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 进行数据的读取，处理分析数据
    //    val lines = spark.sparkContext.textFile("data/2016-10-01_06_p1_invalid.1475274123982.log")
    //    println(lines.map(t => t.split(",", t.length)).filter(_.length >= 85).count())
    //    lines.saveAsTextFile("data/test/out-20190820-04")
    val dfTF: DataFrame = spark.read.text("data/2016-10-01_06_p1_invalid.1475274123982.log")
    dfTF.write.parquet("data/test/out-20190820-05")
    spark.stop()
  }
}
