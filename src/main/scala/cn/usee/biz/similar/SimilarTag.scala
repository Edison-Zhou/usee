package cn.usee.biz.similar

import cn.moretv.doraemon.algorithm.similar.vector.{SimilarVectorAlgorithm, SimilarVectorParameters}
import cn.moretv.doraemon.algorithm.validationCheck.{ValidationCheckAlgorithm, ValidationCheckModel, ValidationCheckParameters}
import cn.usee.biz.BaseClass
import cn.usee.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import cn.whaley.sdk.utils.TransformUDF

/**
  * 基于标签相似度的相似影片推荐
  * Updated by Edison_Zhou on 2019/2/25.
  */
object SimilarTag extends BaseClass {
  def execute(args: Array[String]): Unit = {

    TransformUDF.registerUDFSS

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    //读入节目对应的标签的数据
    val tagSidPath: HdfsPath = new HdfsPath("/data_warehouse/dw_normalized/youkuVideoTagFeature/current",
      "select videoSid as sid, tagFeatures as vector from tmp")
    var tagSidDF = DataReader.read(tagSidPath)
      .selectExpr("sid", "vector")

//    tagSidDF = BizUtils.transferToVirtualSid(tagSidDF, "sid")

    tagSidDF.persist()

    contentTypeList.foreach(contentType => {

      //相似度计算
      val similarAlg: SimilarVectorAlgorithm = new SimilarVectorAlgorithm()
      val similarPara: SimilarVectorParameters = similarAlg.getParameters.asInstanceOf[SimilarVectorParameters]
      similarPara.isSparse = true
      similarPara.topN = 60
      val similarDataMap = Map(similarAlg.INPUT_DATA_KEY -> tagSidDF)
      similarAlg.initInputData(similarDataMap)
      similarAlg.run()
      //结果输出到HDFS
      similarAlg.getOutputModel.output("similarTag/" + contentType)
    })

  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.uSee
}
