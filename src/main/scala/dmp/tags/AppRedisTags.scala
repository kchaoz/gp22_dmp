package dmp.tags

import java.util

import dmp.Utils.{RedisUtils, Tag}
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object AppRedisTags extends Tag {
  /**
    * 打标签统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //    // 解析参数
    //    val row = args(0).asInstanceOf[Row]
    //    // 解析 dict_appname
    //    // 创建 Redis 连接
    //    val jedis = new Jedis("hdp03", 6379)
    //    // 从 Redis中获取数据
    //    val map: util.Map[String, String] = jedis.hgetAll("dict_appname")
    //    // 关闭 Redis 连接
    //    jedis.close()
    //
    //    // appid
    //    val appid = row.getAs[String]("appid")
    //    // appname
    //    val appname = row.getAs[String]("appname")
    //    // 结合 dict_appname 解析处理
    //    // 空值判断与替换（利用dict_appname）
    //    if(appname != ""){
    //      list:+=("APP"+appname,1)
    //    } else {
    //      list:+=("APP"+map.getOrDefault(appid, "不存在"),1)
    //    }
    //    list

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // appid
    val appid = row.getAs[String]("appid")
    // appname
    val appname = row.getAs[String]("appname")

    // 结合 dict_appname 解析处理
    // 空值判断与替换（利用dict_appname）
    if (appname != "") {
      list :+= ("APP" + appname, 1)
    } else {
      // 解析 dict_appname
      // 创建 Redis 连接
      //    val jedis = new Jedis("hdp03", 6379)
      // 从 Redis中获取数据
      //    val appnameFromRedis: String = jedis.hget("dict_appname", appid)
      // 关闭 Redis 连接
      //    jedis.close()
      val appnameFromRedis = RedisUtils.getAppnameFromRedisByAppid(appid)
      list :+= ("APP" + (if (appnameFromRedis != null) appnameFromRedis else "不存在"), 1)
    }
    list
  }

}
