package cn.usee.biz.defaultRecommend

import java.text.SimpleDateFormat
import java.util.Date

import cn.usee.biz.BaseClass
import cn.usee.biz.constant.PathConstants
import cn.usee.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{DateRange, HdfsPath, RedisPath}
import cn.moretv.doraemon.common.util.ArrayUtils
import cn.moretv.doraemon.data.reader.DataReader
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, expr}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Edison_Zhou on 2019/3/12.
  * 详情分频道热门推荐
  */
object DetailDefaultRecommend extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.uSee

  override def execute(args: Array[String]): Unit ={

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {
      val dataReader = new DataReader()

      //获取各频道的热门数据
      val hotLongVideo = BizUtils.getHotRankingList(contentType, 0)
      val hotByType = ArrayUtils.randomTake(hotLongVideo, 200)

      val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
      import ss.implicits._
      /**
        * 各频道热门数据转成DF
        */
      val similarDF = hotByType.map(e => ("detail-" + contentType + "_default", e, 1.0)).
        toList.toDF("sid", "item", "similarity")

      val similarFoldDF = similarDF.groupBy("sid").agg(collect_list("item")).toDF("sid", "id")
        .selectExpr("cast(sid as string) as sid", "id")

      val dataPackParam = new DataPackParam
      dataPackParam.format = FormatTypeEnum.KV
      dataPackParam.extraValueMap = Map("alg" -> "hotRandom","title" -> "相似影片")
      val similarPackDf = DataPack.pack(similarFoldDF, dataPackParam).toDF("key", "similar")

      val dataPackParam2 = new DataPackParam
      dataPackParam2.format = FormatTypeEnum.KV
      dataPackParam2.extraValueMap = Map("date" -> new SimpleDateFormat("yyyyMMdd hh:mm").format(new Date()))

      val detailDefaultPackedDF = DataPack.pack(similarPackDf, dataPackParam2)


      /**
        * 写入redis
        */
      val dataWriter = new DataWriter2Kafka
      val topic = ConfigUtil.get("default.redis.topic")
      val host = ConfigUtil.get("default.redis.host")
      val port = ConfigUtil.getInt("default.redis.port")
      val dbIndex = ConfigUtil.getInt("default.redis.dbindex")
      val ttl = ConfigUtil.getInt("default.redis.ttl")

      val detailFormatType = FormatTypeEnum.KV
      for (subhost <- host.trim.split(",")) {
        val detailRedisPath = RedisPath(topic, subhost, port, dbIndex, ttl, detailFormatType)

        dataWriter.write(detailDefaultPackedDF, detailRedisPath)
      }

    })

  }

}
