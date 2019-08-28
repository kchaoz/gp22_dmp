package dmp.tags

import dmp.Utils.TagsUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object ContextTags {
  def main(args: Array[String]): Unit = {
    // 对输入参数做一个过滤
    if (args.length < 2) {
      println("参数不对！")
      sys.exit(1)
    }
    // 设置输入输出路径
    val Array(inputPath, outPath, dictPath, stopWordPath) = args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    // 读取数据
    val df = spark.read.parquet(inputPath)
    // 读取字典文件
    val dictAppNameMap = spark.sparkContext.textFile(dictPath)
      .map(_.split("\t",-1))
      .filter(_.length>=5)
      .map(arr=>(arr(4),arr(1)))
      .collectAsMap()
    // 广播字典文件
    val brdcstDictAppNameMap = spark.sparkContext.broadcast(dictAppNameMap)
    // 读取停用词库
    val stopWordsMap = spark.sparkContext.textFile(stopWordPath).map((_, 1)).collectAsMap()
    // 广播停用词
    val brdcstStopWordsMap = spark.sparkContext.broadcast(stopWordsMap)
    // 引入上下文隐式转换
    import spark.implicits._
    // 过滤得到符合ID的数据
    df.filter(TagsUtils.userId)
      // 接下来所有的标签都在内部实现
      .map(row => {
      // 获取用户ID
      val userId = TagsUtils.getOneUserId(row)
      // 接下来通过row数据 打上 所有标签（按照需求）
      // 打印广告标签
      val adList = AdTags.makeTags(row)
      // 获取 appname
//      val appnameList = AppBroadcastTags.makeTags(row, brdcstDictAppNameMap)
      val appnameList = AppRedisTags.makeTags(row)
      // 获取渠道ID
      val adplatformprovideridList = AdplatformprovideridTags.makeTags(row)
      // 制作设备相关标签
      // 获取设备操作系统
      val deviceSystemList = DeviceSystemTags.makeTags(row)
      // 获取设备联网方式
      val networkmannernameList = InternetTags.makeTags(row)
      // 设备运营商方式 ispname
      val ispnameList = IspnameTags.makeTags(row)
      // 获取关键字
      val keywords = KeywordsTags.makeTags(row, brdcstStopWordsMap)
      // 获取省市名称
      val provincenameAndCitynameList = LocationTags.makeTags(row)

      (userId, adList ++ appnameList ++ adplatformprovideridList ++
        deviceSystemList ++ networkmannernameList ++ ispnameList ++
        keywords ++ provincenameAndCitynameList)
    })
      // 结果展示
      .rdd.foreach(println)

    spark.stop()
  }
}
