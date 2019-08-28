package dmp.Utils

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

object HbaseUtils {

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
