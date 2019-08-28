package dmp.Utils

import org.apache.spark.sql.{Row, SparkSession}

object GetdataUtils {

  def getDataFrameFromFile(spark: SparkSession, path: String) = {
    // path: data/2016-10-01_06_p1_invalid.1475274123982.log
    val lines = spark.sparkContext.textFile(path)
    // 按要求切割，并且保证数据的长度大于等于85个字段，
    // 如果切割的时候遇到相同切割条件重复的情况下，需要切割的话，那么后面需要加上对应匹配参数
    // 这样切割才会准确 比如 ,,,,,,, 会当成一个字符切割 需要加上对应的匹配参数
    val rowRDD = lines.map(t => t.split(",", t.length)).filter(_.length >= 85)
      .map(arr => {
        Row(
          arr(0),
          TypeUtils.toInt(arr(1)),
          TypeUtils.toInt(arr(2)),
          TypeUtils.toInt(arr(3)),
          TypeUtils.toInt(arr(4)),
          arr(5),
          arr(6),
          TypeUtils.toInt(arr(7)),
          TypeUtils.toInt(arr(8)),
          TypeUtils.toDouble(arr(9)),
          TypeUtils.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          TypeUtils.toInt(arr(17)),
          arr(18),
          arr(19),
          TypeUtils.toInt(arr(20)),
          TypeUtils.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          TypeUtils.toInt(arr(26)),
          arr(27),
          TypeUtils.toInt(arr(28)),
          arr(29),
          TypeUtils.toInt(arr(30)),
          TypeUtils.toInt(arr(31)),
          TypeUtils.toInt(arr(32)),
          arr(33),
          TypeUtils.toInt(arr(34)),
          TypeUtils.toInt(arr(35)),
          TypeUtils.toInt(arr(36)),
          arr(37),
          TypeUtils.toInt(arr(38)),
          TypeUtils.toInt(arr(39)),
          TypeUtils.toDouble(arr(40)),
          TypeUtils.toDouble(arr(41)),
          TypeUtils.toInt(arr(42)),
          arr(43),
          TypeUtils.toDouble(arr(44)),
          TypeUtils.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          TypeUtils.toInt(arr(57)),
          TypeUtils.toDouble(arr(58)),
          TypeUtils.toInt(arr(59)),
          TypeUtils.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          TypeUtils.toInt(arr(73)),
          TypeUtils.toDouble(arr(74)),
          TypeUtils.toDouble(arr(75)),
          TypeUtils.toDouble(arr(76)),
          TypeUtils.toDouble(arr(77)),
          TypeUtils.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          TypeUtils.toInt(arr(84))
        )
      })

    // 构建 DF并返回
    spark.createDataFrame(rowRDD, SchemaUtils.structtype)
  }
}
