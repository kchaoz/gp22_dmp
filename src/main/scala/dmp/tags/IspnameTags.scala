package dmp.tags

import dmp.Utils.Tag
import org.apache.spark.sql.Row

object IspnameTags extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
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

  /**
    * 未知, 1788.0,1770.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
    * 电信, 68.0,68.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
    * 移动, 81.0,81.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
    * 联通, 63.0,63.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
    */
}

