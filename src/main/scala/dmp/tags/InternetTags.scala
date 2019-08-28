package dmp.tags

import dmp.Utils.Tag
import org.apache.spark.sql.Row

object InternetTags extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 设备联网方式 InternetName
    val networkmannername = row.getAs[String]("networkmannername")
    // 对获取的 InternetName 进行相应的处理
    if(networkmannername.equals("Wifi")){
      list:+=("WIFI D00020001",1)
    }else if(networkmannername.equals("4G")){
      list:+=("4G D00020002",1)
    }else if(networkmannername.equals("3G")){
      list:+=("3G D00020003",1)
    }else if(networkmannername.equals("2G")){
      list:+=("2G D00020004",1)
    }else{
      list:+=("_ D00020005",1)
    }
    list
  }
}

/**
  * 3G, 39.0,39.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
  * 2G, 50.0,50.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
  * 未知, 276.0,263.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
  * Wifi, 1539.0,1534.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
  * 4G, 96.0,96.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
  */
