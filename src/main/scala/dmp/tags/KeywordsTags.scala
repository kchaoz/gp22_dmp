package dmp.tags

import dmp.Utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object KeywordsTags extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 关键字
    val keywords = row.getAs[String]("keywords")
    // 获取停用词
    val brdcstStopWordsMap = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]

    // 对关键字进行过滤
    if (keywords.contains("|")) {
      val fields = keywords.split("\\|")
      for(field <- fields){
        if(field.size >= 3 && field.size <= 8 && !brdcstStopWordsMap.value.contains(field)) {
          list :+= ("K" + field, 1)
        }
      }
    } else {
      if(keywords.size >= 3 && keywords.size <= 8 && !brdcstStopWordsMap.value.contains(keywords)){
        list :+= ("K" + keywords, 1)
      }
    }
    list
  }

}
