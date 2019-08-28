package dmp.tags

import com.typesafe.config.ConfigFactory
import dmp.Utils.TagsUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object ContextTagsAndHbase {
  def main(args: Array[String]): Unit = {
    if(args.length != 5){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath,dirPath,stopPath,days)=args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    // todo 调用Hbase API
    // 加载配置文件
//    val load = ConfigFactory.load()
//    val hbaseTableName = load.getString("hbase.tableName")
//    // 创建Hadoop任务
//    val configuration = spark.sparkContext.hadoopConfiguration
//    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
//    // 创建HbaseConnection
//    val hbconn = ConnectionFactory.createConnection(configuration)
//    val hbadmin = hbconn.getAdmin
//    // 判断表是否可用
//    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
//      // 创建表操作
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
//      val descriptor = new HColumnDescriptor("tags")
//      tableDescriptor.addFamily(descriptor)
//      hbadmin.createTable(tableDescriptor)
//      hbadmin.close()
//      hbconn.close()
//    }
//    // 创建JobConf
//    val jobconf = new JobConf(configuration)
//    // 指定输出类型和表
//    jobconf.setOutputFormat(classOf[TableOutputFormat])
//    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)
//    // 读取数据
    val df = spark.read.parquet(inputPath)
//    // 读取字段文件
//    val map = spark.sparkContext.textFile(dirPath).map(_.split("\t",-1))
//      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
//    // 将处理好的数据广播
//    val broadcast = spark.sparkContext.broadcast(map)
//
//    // 获取停用词库
//    val stopword = spark.sparkContext.textFile(stopPath).map((_,0)).collectAsMap()
//    val bcstopword = spark.sparkContext.broadcast(stopword)
    // 引入spark隐式转换
    import spark.implicits._
    // 过滤符合Id的数据
    df
      .filter(TagsUtils.userId).toDF
      // 接下来所有的标签都在内部实现
      .map(row=>{
      // 取出用户Id
//      val userId = TagsUtils.getOneUserId(row)
//      // 接下来通过row数据 打上 所有标签（按照需求）
//      val adList = AdTags.makeTags(row)
//      val appList = AppBroadcastTags.makeTags(row,broadcast)
//      val keywordList = KeywordsTags.makeTags(row,bcstopword)
////      val dvList = TagDevice.makeTags(row)
//      val loactionList = LocationTags.makeTags(row)
      val business = BusinessTags.makeTags(row)
//      (userId,adList++appList++keywordList++loactionList++business)
      business
  }).rdd
      .filter(_.nonEmpty)
//      .reduceByKey((list1,list2)=>
//        // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
//        (list1:::list2)
//          // List(("APP爱奇艺",List()))
//          .groupBy(_._1)
//          .mapValues(_.foldLeft[Int](0)(_+_._2))
//          .toList
//      )
//      .map{
//      case(userid,userTag)=>{
//
//        val put = new Put(Bytes.toBytes(userid))
//        // 处理下标签
//        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
//        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
//        (new ImmutableBytesWritable(),put)
//      }
//    }
      .foreach(println)
      // 保存到对应表中
//      .saveAsHadoopDataset(jobconf)

  }
}
