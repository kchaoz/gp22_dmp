package dmp.tags

import dmp.Utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object AppBroadcastTags extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 解析 dict_appname
    val brdcstDictAppNameMap = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

    // appid
    val appid = row.getAs[String]("appid")
    // appname
    val appname = row.getAs[String]("appname")
    // 结合 dict_appname解析处理
    // 空值判断与替换（利用dict_appname）
    if(appname != ""){
      list:+=("APP"+appname,1)
    } else {
      list:+=("APP"+brdcstDictAppNameMap.value.getOrElse(appid, "不存在"),1)
    }
    list
  }
}
