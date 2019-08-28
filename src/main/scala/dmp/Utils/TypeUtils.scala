package dmp.Utils

object TypeUtils {

  def toInt(str: String): Int = {
    try{
      str.toInt
    }catch {
      case e: Exception => 0
    }
  }

  def toDouble(str: String): Double = {
    try{
      str.toDouble
    }catch {
      case e: Exception => 0.0
    }
  }
}
