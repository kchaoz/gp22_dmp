package dmp.tags

import dmp.Utils.Tag
import org.apache.spark.sql.Row

object DeviceSystemTags extends Tag {
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

    list
  }
}
