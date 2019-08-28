package dmp.tags

import dmp.Utils.Tag
import org.apache.spark.sql.Row

object DeviceTags extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 设备操作系统
    val client = row.getAs[Int]("client")
    // 按照操作系统ID进行相应处理
    if(client == 1){
      list:+=("1 Android D0001000"+client,1)
    }else if(client == 2){
      list:+=("2 IOS D0001000"+client,1)
    }else if(client == 3){
      list:+=("3 WinPhone D0001000"+client, 1)
    }else{
      list:+=("_ 其 他 D00010004", 1)
    }

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

    // 设备运营商方式 ispname
    val ispname = row.getAs[String]("ispname")
    // 对获取的 InternetName 进行相应的处理
    if(ispname.equals("移动")){
      list:+=("移 动 D00030001 ",1)
    }else if(ispname.equals("联通")){
      list:+=("联 通 D00030002",1)
    }else if(ispname.equals("电信")){
      list:+=("电 信 D00030003",1)
    }else{
      list:+=("_ D00030004",1)
    }

    list
  }
}
