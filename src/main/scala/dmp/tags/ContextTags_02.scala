package dmp.tags

import dmp.Utils.TagsUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object ContextTags_02 {
  def main(args: Array[String]): Unit = {
    // 对输入参数做一个过滤
    if (args.length < 2) {
      println("参数不对！")
      sys.exit(1)
    }
    // 设置输入输出路径
    val Array(inputPath, outPath, dictPath, stopWordPath, days) = args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    // 读取数据
    val df = spark.read.parquet(inputPath)
    // 读取字典文件
    val dictAppNameMap = spark.sparkContext.textFile(dictPath)
      .map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collectAsMap()
    // 广播字典文件
    val brdcstDictAppNameMap: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(dictAppNameMap)
    // 读取停用词库
    val stopWordsMap = spark.sparkContext.textFile(stopWordPath).map((_, 1)).collectAsMap()
    // 广播停用词
    val brdcstStopWordsMap: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(stopWordsMap)
    // 引入上下文隐式转换
    import spark.implicits._
    // 过滤得到符合ID的数据
    val baseRDD: RDD[(List[String], Row)] = df.filter(TagsUtils.userId).rdd
      // 接下来所有的标签都在内部实现
      .map(row => {
      val userList: List[String] = TagsUtils.getAllUserId(row)
      (userList, row)
    })
    // 构建点集合
    val vertiesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2
      // 所有标签
      val adList = AdTags.makeTags(row)
//      val appList = AppBroadcastTags.makeTags(row, brdcstDictAppNameMap)
//      val keywordList = KeywordsTags.makeTags(row, stopWordsMap)
      val dvList = DeviceTags.makeTags(row)
      val loactionList = LocationTags.makeTags(row)
      val business = BusinessTags.makeTags(row)
//      val AllTag = adList ++ appList ++ keywordList ++ dvList ++ loactionList ++ business
      val AllTag = adList ++ dvList ++ loactionList ++ business
      // List((String,Int))
      // 保证其中一个点携带者所有标签，同时也保留所有userId
      val VD = tp._1.map((_, 0)) ++ AllTag
      // 处理所有的点集合
      tp._1.map(uId => {
        // 保证一个点携带标签 (uid,vd),(uid,list()),(uid,list())
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
    // vertiesRDD.take(50).foreach(println)
    // 构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      // A B C : A->B A->C
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
    })
    //edges.take(20).foreach(println)
    // 构建图
    val graph = Graph(vertiesRDD,edges)
    // 取出顶点 使用的是图计算中的连通图算法
    val vertices = graph.connectedComponents().vertices
//    vertices.take(1).foreach(println)
    // 处理所有的标签和id
    vertices.join(vertiesRDD)
      .map{
      case (uId,(conId,tagsAll))=>(conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      // 聚合所有的标签
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      .take(6).foreach(println)


    spark.stop()
  }
}
