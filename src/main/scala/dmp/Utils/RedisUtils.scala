package dmp.Utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtils {

  /**
    * 从redis连接池中获取一个 Jedis 连接
    */
  object JedisConnectionPool {

    val config = new JedisPoolConfig()

    // 设置最大连接数
    config.setMaxTotal(20)
    // 最大空闲
    config.setMaxIdle(10)
    // 创建连接
    val pool = new JedisPool(config, "hdp03", 6379, 10000)

    def getConnection(): Jedis = {
      pool.getResource
    }
  }

  /**
    * 存储到 Redis
    *
    * @param spark
    * @param path
    */
  def data2Redis(spark: SparkSession, path: String): Unit = {
    val data2Redis: RDD[(String, String)] = spark.sparkContext.textFile(path)
      .map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))

    val redisConn = (it: Iterator[(String, String)]) => {
      val jedis = new Jedis("hdp03", 6379)
      it.foreach(tup => {
        jedis.hset("dict_appname", tup._1, tup._2)
      })

      jedis.close()
    }

    data2Redis.foreachPartition(redisConn)
  }

  def getAppnameFromRedisByAppid(appid: String): String = {
    // 创建 Redis 连接
    val jedis = new Jedis("hdp03", 6379)
    // 从 Redis中获取数据
    val appnameFromRedis: String = jedis.hget("dict_appname", appid)
    // 关闭 Redis 连接
    jedis.close()

    appnameFromRedis
  }

}
