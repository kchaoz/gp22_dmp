package dmp.tags

import dmp.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AdplatformprovideridTags extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 渠道 ID
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+adplatformproviderid,1)
    list
  }

}
