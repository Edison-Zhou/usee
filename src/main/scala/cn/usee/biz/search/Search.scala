package cn.usee.biz.search


import java.text.SimpleDateFormat
import java.util.Date

import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.usee.biz.BaseClass
import cn.usee.biz.util.BizUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by cheng.huan on 2019/03/06.
  */
object Search extends BaseClass {

  override def execute(args: Array[String]): Unit = {
    //有效影片的数据
    val validSidPath = BizUtils.getMysqlPath4Test("long_valid_sid")
    val longVideoDf = deDuplicateByVirtualSid(DataReader.read(validSidPath).toDF("sid", "contentType", "title", "riskFlag"))

    val shortSidPath = BizUtils.getMysqlPath4Test("short_valid_sid")
    val shortVideoDf = DataReader.read(shortSidPath).toDF("sid","contentType", "title", "riskFlag","update_time")
      .filter(s"!(contentType in ('sports', 'hot') and update_time < '${new SimpleDateFormat("yyyy-MM-dd").format(DateUtils.addDays(new Date(), -180))}')")
      .select("sid", "contentType", "title", "riskFlag")

    //longVideoDf.filter("sid in ('tvwye5p834de')").show(10, false)

    val subjectSidPath = BizUtils.getMysqlPath4Test("valid_subject")
    val subjectVideoDf = DataReader.read(subjectSidPath).toDF("code", "title")

    val starSidPath = BizUtils.getMysqlPath4Test("valid_person")
    val starVideoDf = DataReader.read(starSidPath).toDF("sid", "name")

    BizUtils.getDataFrameInfo(longVideoDf, "longVideoDF")
    BizUtils.getDataFrameInfo(shortVideoDf, "shortVideoDF")
    BizUtils.getDataFrameInfo(subjectVideoDf, "subjectVideoDF")
    BizUtils.getDataFrameInfo(starVideoDf, "starVideoDF")

    val ss = spark
    import ss.implicits._
    val searchWordDf = SearchAlg.recall(longVideoDf, shortVideoDf, subjectVideoDf, starVideoDf)

    searchWordDf.show(100, false)

    //开始排序阶段
    //val date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(), -1))

    //val videoScore = sqlContext.sql(s"select sid, video_score from dws_medusa_bi.medusa_video_score where day_p = '$date'")

    //置顶用的白名单
    /*val toppingDF = List(("tvwycec38sv0", "hot", 0, "dsm", "电视猫", 0.95),
      ("tvwycec3qtp8", "hot", 0, "dsm", "电视猫", 0.95),
      ("tvwy5i9vrsxy", "hot", 0, "dsm", "电视猫", 1.0),
      ("tvwy5i9vrsxy", "hot", 0, "hxc", "贺新春", 1.0),
      ("tvwy5i9vrsxy", "hot", 0, "qxhxc", "群星贺新春", 1.0),
      ("tvwy5i9vrsxy", "hot", 0, "qx", "群星", 1.0))
      .toDF("sid", "contentType", "riskFlag", "searchKey", "highlight", "matchScore")*/

    val beforeReorderDf = searchWordDf
      .toDF("sid", "contentType", "riskFlag", "searchKey", "highlight", "score")
      //.union(toppingDF)
      .groupBy("searchKey", "contentType")
      .agg(collect_list(concat_ws("_", col("sid"), col("highlight"), col("score"), col("riskFlag"))).as("content"))

    //beforeReorderDf.filter("searchKey in ('dsm')").show(100, false)

    val afterReorderDf = SearchAlg.reorder(beforeReorderDf).persist()

    BizUtils.outputWrite(afterReorderDf, "pinyinSearch")


    //输出结果到ES
    val biz = "pinyin_search"
    val alg = "rule0"
    SearchUtil.outputBatch(changeFormat(afterReorderDf, 0), "giraffa_search_low_risk", "IndexKeyWordKey:MINDEXLOW_", biz, alg)
    SearchUtil.outputBatch(changeFormat(afterReorderDf, 1), "giraffa_search_middle_risk", "IndexKeyWordKey:MINDEXHIGH_", biz, alg)
    SearchUtil.outputBatch(changeFormat(afterReorderDf, 2), "giraffa_search_high_risk", "IndexKeyWordKey:MINDEXSUPERHIGH_", biz, alg)
    afterReorderDf.unpersist()
  }

  def changeFormat(df: DataFrame, risk: Int): DataFrame = {
    val ss = spark
    import ss.implicits._
    df.where("content is not null")
      .map(r => (r.getAs[String]("searchKey"), r.getAs[String]("contentType"),
        r.getAs[Seq[String]]("content")
          .map(s => s.split("_"))
          .filter(s => s.length == 4)
          .filter(s => (s(3).toInt + risk) <= 2)
          .sortBy(s => -1 * s(2).toDouble).take(25)
          .map(s => s(0) + "_" + s(1) + "_" + s(2))
          .mkString("|"))
      ).toDF("searchKey", "contentType", "content")
      .groupBy("searchKey").agg(collect_list(struct("contentType", "content")).as("value"))
      .repartition(20)
  }

  /**
    * 用虚拟sid去重，结果保留真实sid
    * @param input DF("sid", "contentType", "title", "riskFlag")
    * @return
    */
  def deDuplicateByVirtualSid(input: DataFrame): DataFrame = {
    val sidRel = BizUtils.readVirtualSidRelation()
    val sidColumnName = "sid"
    input.as("a").join(sidRel.as("b"), expr(s"a.$sidColumnName = b.sid"), "leftouter").drop(expr("b.sid"))
      .withColumn("virtual_sid", expr(s"case when b.virtual_sid is not null then b.virtual_sid else a.$sidColumnName end"))
      .dropDuplicates("virtual_sid")
      .drop(expr("b.virtual_sid"))
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.uSee

}

