package dmp.Utils

trait Tag {

  /**
    * 打标签统一接口
    */
  def makeTags(args: Any*): List[(String, Int)]
}
