package dmp

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Exam18Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val rddFile: RDD[String] = spark.sparkContext.textFile("data/json.txt")

    val nameArrayRdd: RDD[(String, Int)] = rddFile.flatMap(jsonStr => {
      val businessAreasArray: Array[String] = getBusinessFromAmap(jsonStr).split(",")
      businessAreasArray.map(field => (field, 1))
    })

    val nameAndItRdd: RDD[(String, Iterable[(String, Int)])] = nameArrayRdd.groupBy(_._1)

    println("First: ")
    nameAndItRdd.mapValues(_.toList.size).foreach(println)


    val typeArrayRdd: RDD[(String, Int)] = rddFile.flatMap(jsonStr => {
      val businessAreasArray: Array[String] = getTypeFromAmap(jsonStr).split(",")
      businessAreasArray.map(field => (field, 1))
    })

    val typeAndItRdd: RDD[(String, Iterable[(String, Int)])] = typeArrayRdd.groupBy(_._1)
    println("Second: ")
    typeAndItRdd.mapValues(_.toList.size).foreach(println)


    spark.stop()
  }

  def getBusinessFromAmap(jsonStr: String): String = {
    // 解析json串
    val jsonparse = JSON.parseObject(jsonStr)
    // 判断状态是否成功
    val status = jsonparse.getIntValue("status")
    if(status == 0) return ""
    // 接下来解析内部json串，判断每个key的value都不能为空
    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val poisArray = regeocodeJson.getJSONArray("pois")
    if(poisArray == null || poisArray.isEmpty) return null
    // 创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    // 循环输出
    for(item <- poisArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("businessarea"))
      }
    }
    buffer.mkString(",")
  }

  def getTypeFromAmap(jsonStr: String): String = {
    // 解析json串
    val jsonparse = JSON.parseObject(jsonStr)
    // 判断状态是否成功
    val status = jsonparse.getIntValue("status")
    if(status == 0) return ""
    // 接下来解析内部json串，判断每个key的value都不能为空
    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val poisArray = regeocodeJson.getJSONArray("pois")
    if(poisArray == null || poisArray.isEmpty) return null

    // 创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    // 循环输出
    for(item <- poisArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        val typesStr = json.getString("type").split(";")
        for(ele <- typesStr){
          buffer.append(ele)
        }
      }
    }
    buffer.mkString(",")
  }
}
