package cn.usee.biz.similar

import cn.moretv.doraemon.algorithm.similar.cluster.{SimilarClusterAlgorithm, SimilarClusterParameters}
import cn.usee.biz.BaseClass
import cn.usee.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.functions.expr

/**
  * 相似影片默认推荐
  * Reused by Edison_Zhou on 2019/2/22
  */
object SimilarDefault extends BaseClass {

  def execute(args: Array[String]): Unit = {

    TransformUDF.registerUDFSS

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {

      val hotDf = DataReader.read(
        new MysqlPath("bigdata-appsvr-130-6", 3306, "europa",
          "tag_youku_program", "bislave", "slave4bi@whaley",
          "youku_rating >= 8 and status = 1 and risk_flag = 0 and type = 1")).
        select("sid", "content_type").toDF("sid", "cluster")


      //有效影片的数据
      val validSidDF = BizUtils.getAvailableVideo(contentType)
        .selectExpr("sid", s"'$contentType' as cluster")

      //调用
      val similarAlg: SimilarClusterAlgorithm = new SimilarClusterAlgorithm()
      val similarClusterPara = similarAlg.getParameters.asInstanceOf[SimilarClusterParameters]
      similarClusterPara.preferTableUserColumn = "sid"
      similarClusterPara.clusterDetailTableContentColumn = "sid"
      similarClusterPara.outputUserColumn = "sid"
      similarClusterPara.topN = 70

      val dataMap = Map(similarAlg.INPUT_DATA_KEY_PREFER -> hotDf,
        similarAlg.INPUT_DATA_KEY_CLUSTER_DETAIL -> validSidDF)
      similarAlg.initInputData(dataMap)

      similarAlg.run()
      //结果输出到HDFS
      similarAlg.getOutputModel.output("similarDefault/" + contentType)
    })
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.uSee
}
