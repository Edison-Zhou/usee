package cn.usee.biz.detail

import java.text.SimpleDateFormat
import java.util.Date

import cn.usee.biz.BaseClass
import cn.usee.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.RedisPath
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.functions._

/**
  * Created by Edison_Zhou on 2019/3/23.
  */
object DetailPageRecommend extends BaseClass {
  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.uSee

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {

      val similarDf = DataReader.read(BizUtils.getHdfsPathForRead("similarMix/" + contentType))

      similarDf.toDF("sid","item","score").createOrReplaceTempView(s"similar_${contentType}_tmp")


      val similarSql =
        s"""
           |select a.sid,a.item from (
           |  select sid,item,score,
           |  row_number() over(partition by sid order by score desc) as rn
           |  from similar_${contentType}_tmp
           |) a
           | where a.rn <=24
        """.stripMargin
      val similarFoldDf = sqlContext.sql(similarSql).groupBy(expr("sid")).agg(collect_list("item").alias("id"))
        .selectExpr("cast(sid as string) as sid", "id")


      val dataPackParam = new DataPackParam
      dataPackParam.format = FormatTypeEnum.KV
      dataPackParam.extraValueMap = Map("alg" -> "mix")
      val similarPackDf = DataPack.pack(similarFoldDf, dataPackParam).toDF("key", "similar")
      
      val dataPackParam2 = new DataPackParam
      dataPackParam2.format = FormatTypeEnum.KV
      dataPackParam2.extraValueMap = Map("date" -> new SimpleDateFormat("yyyyMMdd").format(new Date()))

      var joinDf = similarPackDf.as("a").where("a.key is not null")

      val result = DataPack.pack(joinDf, dataPackParam2)

      val dataWriter = new DataWriter2Kafka

      val topic = ConfigUtil.get("detail.redis.topic")
      val host = ConfigUtil.get("detail.redis.host")
      val port = ConfigUtil.getInt("detail.redis.port")
      val dbIndex = ConfigUtil.getInt("detail.redis.dbindex")
      val ttl = ConfigUtil.getInt("detail.redis.ttl")
      val formatType = FormatTypeEnum.KV
      for (subhost <- host.trim.split(",")) {
        val path = RedisPath(topic, subhost, port, dbIndex, ttl, formatType)

        dataWriter.write(result, path)
      }
    })

  }
}
