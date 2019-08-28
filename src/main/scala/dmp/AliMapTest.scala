package dmp

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object AliMapTest {
  def main(args: Array[String]): Unit = {
    val bninfo = getBusinessFromAmap(116.310003,39.991957)
    println(bninfo)
  }

  // 获取高德地图商圈信息
  def getBusinessFromAmap(long:Double,lat:Double):String={
    //https://restapi.amap.com/v3/geocode
    // /regeo?output=xml&location=116.310003,39.991957&key=<用户的key>&radius=1000&extensions=all
    val location = long+","+lat
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=9b3768efd6e69c4e5943b02083fe4fc0"
    // 调用请求
     val jsonstr = get(urlStr)  // JSON字符串(此句报错)
//    val jsonstr = "{\"status\":\"1\",\"regeocode\":{\"addressComponent\":{\"city\":[],\"province\":\"北京市\",\"adcode\":\"110108\",\"district\":\"海淀区\",\"towncode\":\"110108015000\",\"streetNumber\":{\"number\":\"5号\",\"location\":\"116.310454,39.9927339\",\"direction\":\"东北\",\"distance\":\"94.5489\",\"street\":\"颐和园路\"},\"country\":\"中国\",\"township\":\"燕园街道\",\"businessAreas\":[{\"location\":\"116.303364,39.97641\",\"name\":\"万泉河\",\"id\":\"110108\"},{\"location\":\"116.314222,39.98249\",\"name\":\"中关村\",\"id\":\"110108\"},{\"location\":\"116.294214,39.99685\",\"name\":\"西苑\",\"id\":\"110108\"}],\"building\":{\"name\":\"北京大学\",\"type\":\"科教文化服务;学校;高等院校\"},\"neighborhood\":{\"name\":\"北京大学\",\"type\":\"科教文化服务;学校;高等院校\"},\"citycode\":\"010\"},\"formatted_address\":\"北京市海淀区燕园街道北京大学\"},\"info\":\"OK\",\"infocode\":\"10000\"}"
    // 解析json串
    val jsonparse = JSON.parseObject(jsonstr)
    // 判断状态是否成功
    val status = jsonparse.getIntValue("status")
    if(status == 0) return ""
    // 接下来解析内部json串，判断每个key的value都不能为空
    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val addressComponentJson = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    val businessAreasArray = addressComponentJson.getJSONArray("businessAreas")
    if(businessAreasArray == null || businessAreasArray.isEmpty) return null
    // 创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    // 循环输出
    for(item <- businessAreasArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }

  // GET请求
  def get(url:String):String={
    val client = HttpClients.createDefault()
    val get = new HttpGet(url)
    // 发送请求
    val response: CloseableHttpResponse = client.execute(get)
    // 获取返回结果
    EntityUtils.toString(response.getEntity,"UTF-8")
  }

  def getBusinessFromAmap2(jsonStr: String): String = {
    // 解析json串
    val jsonparse = JSON.parseObject(jsonStr)
    // 判断状态是否成功
    val status = jsonparse.getIntValue("status")
    if(status == 0) return ""
    // 接下来解析内部json串，判断每个key的value都不能为空
    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val addressComponentJson = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    val businessAreasArray = addressComponentJson.getJSONArray("businessAreas")
    if(businessAreasArray == null || businessAreasArray.isEmpty) return null
    // 创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    // 循环输出
    for(item <- businessAreasArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }

}
