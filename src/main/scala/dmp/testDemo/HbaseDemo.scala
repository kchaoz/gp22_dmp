package dmp.testDemo

import com.typesafe.config.ConfigFactory
import dmp.Utils.TagsUtils
import dmp.tags.{AdTags, AdplatformprovideridTags, AppRedisTags, DeviceSystemTags, InternetTags, IspnameTags, KeywordsTags, LocationTags}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object HbaseDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    getDfData(spark).rdd.map(tup => (tup._1, tup._2.mkString("+")))
//      .foreach(println)
        .map(tup => {
      val put = new Put(Bytes.toBytes(tup._1))
      // 处理下标签
      val tags = tup._2
      put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes("20190826"),Bytes.toBytes(tags))
      (new ImmutableBytesWritable(),put)
    })
        .saveAsHadoopDataset(hbaseOperatorAndGetHbaseJobconf(spark))


    spark.stop
  }

  def getDfData(spark: SparkSession): Dataset[(String, List[(String, Int)])] = {
    // 读取数据
    val df = spark.read.parquet("data/in/in-20190820-01")
    // 读取字典文件
    val dictAppNameMap = spark.sparkContext.textFile("data/app_dict.txt")
      .map(_.split("\t",-1))
      .filter(_.length>=5)
      .map(arr=>(arr(4),arr(1)))
      .collectAsMap()
    // 广播字典文件
    val brdcstDictAppNameMap = spark.sparkContext.broadcast(dictAppNameMap)
    // 读取停用词库
    val stopWordsMap = spark.sparkContext.textFile("data/stopwords.txt").map((_, 1)).collectAsMap()
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
  }

  def hbaseOperatorAndGetHbaseJobconf(spark: SparkSession): JobConf = {
    val sc = spark.sparkContext
    // 调用 Hbase API
    // 加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.tableName")
    // 创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    // 创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    // 判断表是否可用
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      // 创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val columnDescriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(columnDescriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    // 创建JobConf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    jobconf
  }

}
