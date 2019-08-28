package dmp.Utils

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

object PersistUtils {

  def rdd2Mysql(it: Iterator[(String, List[Double])]) = {
    var conn: Connection = null
    var ps: PreparedStatement = null

    val sql = "insert into media_analysis_info " +
      "(appname, origin_request_count, effective_request_count, ad_request_count, join_bid_count, bid_success_count, show_count, click_count, dsp_ad_consume, dsp_ad_cost) " +
      "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    val jdbcUrl = "jdbc:mysql://hdp03:3306/mydb01?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val password = "123456"

    try {
      conn = DriverManager.getConnection(jdbcUrl, user, password)
      it.foreach(tup => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, tup._1)
        ps.setDouble(2, tup._2(0))
        ps.setDouble(3, tup._2(1))
        ps.setDouble(4, tup._2(2))
        ps.setDouble(5, tup._2(3))
        ps.setDouble(6, tup._2(4))
        ps.setDouble(7, tup._2(5))
        ps.setDouble(8, tup._2(6))
        ps.setDouble(9, tup._2(7))
        ps.setDouble(10, tup._2(8))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => e.printStackTrace
    } finally {
      if (ps != null)
        ps.close
      if (conn != null)
        conn.close
    }
  }
}
