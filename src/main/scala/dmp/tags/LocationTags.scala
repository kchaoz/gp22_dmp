package dmp.tags

import dmp.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object LocationTags extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取省份名称
    val provincename = row.getAs[String]("provincename")
    // 获取城市名称
    val cityname = row.getAs[String]("cityname")
    // 按需求进行相应的处理
    list :+= ("ZP" + provincename, 1)
    list :+= ("ZC" + cityname, 1)
    list
  }
}
