package cn.usee.biz.defaultRecommend

import cn.usee.biz.{BaseClass, util}
import cn.usee.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.CouchbasePath
import cn.moretv.doraemon.common.util.{ArrayUtils, DateUtils}
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * Reused by Edison_Zhou on 2019/3/10.
  */
object DefaultRecommend extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.uSee

  override def execute(args: Array[String]): Unit = {

    val dataWriter = new DataWriter2Kafka


    //数据3、最热榜
    val contentTypeList = List(("movie",400), ("tv",400), ("zongyi",200), ("comic",200), ("kids",200), ("jilu",200))
    var hotVidioUnion = new ArrayBuffer[String]
    contentTypeList.foreach(e => {
      var contentType = e._1
      var numOfEachType = e._2
      var hotVideoByType= ArrayUtils.randomTake(BizUtils.getHotRankingList(contentType, 0), numOfEachType)
      hotVidioUnion ++= hotVideoByType
    })
    val hotLongVideo = hotVidioUnion.toArray
    println("数组hotLongVideo的长度是：")
    println(hotLongVideo.length)
    //数据4、聚类结果

    //数据5.TOP-N vip sid
    val hotVip = BizUtils.getHotRankingVipList("all", 0)

    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    /**
      * 会员看看默认推荐
      */
    val vipDefaultArr = ArrayUtils.randomTake(hotVip, 200)
    val vipDefaultDF = vipDefaultArr.toList.map(e => ("default", e)).toDF("uid", "sid")
    val vipFoldDF = vipDefaultDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id")
      .selectExpr("cast(uid as string) as uid", "id")

    val dataPackParam = new DataPackParam
    dataPackParam.format = FormatTypeEnum.KV
    dataPackParam.extraValueMap = Map("alg" -> "randomVip")
    val vipPackDF = DataPack.pack(vipFoldDF, dataPackParam).toDF("key", "vip")

    val dataPackParam2 = new DataPackParam
    dataPackParam2.format = FormatTypeEnum.KV
    dataPackParam2.keyPrefix = "p:a:"
    dataPackParam2.extraValueMap = Map("date" -> DateUtils.getTimeStamp)
    val vipDefaultPacked = DataPack.pack(vipPackDF, dataPackParam2)
    vipDefaultPacked.collect().foreach(println)

    for (i <- 1 to 100) {
    dataWriter.write(vipDefaultPacked, new CouchbasePath(ConfigUtil.get("couchbase.usee.topic"), 172800))}


    /* 兴趣默认推荐
    方案：长视频最热榜*/
    val interestDefault = ArrayUtils.randomTake(hotLongVideo, 200)
    BizUtils.defaultRecommend2Kafka4Couchbase("p:i:", interestDefault, "hotRandom",
      ConfigUtil.get("couchbase.usee.topic"), 200)
  }
}
