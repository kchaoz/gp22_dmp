package dmp

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.stop()
  }

  val dql03 =
    """
      select
        dense_rank() over(partition by class order by score desc) - 1 class_score_rank
      from scene_practice.student
    """

  val dql02 =
    """
        select
          class `班级`,
          sid,
          score,
          class_score_rank,
          score - nvl(lag(score) over(partition by class order by class_score_rank), 0) score_diff,
          score - (lag(score, 1, 0) over(partition by class order by class_score_rank)) score_diff_bak
        from(
          select
            sid,
            class,
            score,
            dense_rank() over(partition by class order by score desc) class_score_rank
          from scene_practice.student
        ) tmp
        where class_score_rank < 4
    """

  val ddl02 = "drop table scene_practice.student"

  val dbsql = "create database if not exists scene_practice"

  val ddlsql =
    """
        create external table scene_practice.student(
          sid int,
          class int,
          score int
        )
        row format delimited fields terminated by ','
        location 'data/in/student/'
    """
  val dql01 = "select * from scene_practice.student"
}
