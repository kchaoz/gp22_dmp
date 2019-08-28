package dmp.sparksql

import java.util
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object ProAndCityCountSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val allDataRDD = getAllDataRDD(spark, "data/in/in-20190820-01")
    generateProvinceAndCityNameTable(spark, allDataRDD)
    val resDF = getResultDF(spark)
    resDF.show()
//    resDF.write.json("data/out/out-20190820-02")
//    persistResult2Mysql(resDF)
//    persist2Textfile(resDF)

    spark.stop()
  }

  def getAllDataRDD(spark: SparkSession, data: String) = {
    spark.read.parquet(data).rdd // data/in-20190820-01
  }

  def getProvinceAndCityNameRDD(allDataRDD: RDD[Row]) = {
    allDataRDD.map(row => {
      val provinceName = row.getString(24)
      val cityName = row.getString(25)
      Row(provinceName, cityName)
    })
  }

  def generateProvinceAndCityNameTable(spark: SparkSession, allDataRDD: RDD[Row]) = {
    // 获取需要字段（provinceName, cityName）的 RDD[Row]
    val provinceAndCityNameRDD = getProvinceAndCityNameRDD(allDataRDD)

    // 构建schema信息
    val structFields = new util.ArrayList[StructField]
    structFields.add(DataTypes.createStructField("provincename", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("cityname", DataTypes.StringType, true))

    val schema = DataTypes.createStructType(structFields)

    // 生成DataFrame
    val df = spark.createDataFrame(provinceAndCityNameRDD, schema)
    // 生成 province_and_city_name_info 临时表
    df.createTempView("province_and_city_name_info")
  }

  def getResultDF(spark: SparkSession) = {
    val sql =
      "select " +
        "count(1) ct, " +
        "provincename, " +
        "cityname " +
      "from province_and_city_name_info " +
      "group by " +
        "provincename, " +
        "cityname " +
       "order by ct desc"

    spark.sql(sql)
  }

  def persistResult2Mysql(resDF: DataFrame) = {
    val (prop, url) = getProAndUrl()
    resDF.write.mode(SaveMode.Append).jdbc(url, "province_and_city_name_info", prop)
  }

  def persist2Textfile(resDF: DataFrame) = {
    resDF.rdd.map(row => {
      val ct = row.getLong(0)
      val provincename = row.getString(1)
      val cityname = row.getString(2)
      (ct, provincename, cityname)
    }).saveAsTextFile("data/out/out-20190820-03")
  }

  def getProAndUrl() = {
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    val url = "jdbc:mysql://hdp03:3306/mydb01?useUnicode=true&characterEncoding=utf8"
    (prop, url)
  }

  def infoAndNotice() = {
    /**
      * mysql建表语句：province_and_city_name_info,需指定编码utf8个数，否则汉语会发生乱码
      *
        DROP TABLE IF EXISTS `province_and_city_name_info`;
        CREATE TABLE `province_and_city_name_info` (
        `ct` int(11) DEFAULT NULL,
        `provincename` varchar(255) DEFAULT NULL,
        `cityname` varchar(255) DEFAULT NULL
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
      */
  }

}
