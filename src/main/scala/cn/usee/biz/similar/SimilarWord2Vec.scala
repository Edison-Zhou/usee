package cn.usee.biz.similar

import cn.moretv.doraemon.algorithm.similar.vector.{SimilarVectorAlgorithm, SimilarVectorParameters}
import cn.moretv.doraemon.algorithm.validationCheck.{ValidationCheckAlgorithm, ValidationCheckModel, ValidationCheckParameters}
import cn.usee.biz.BaseClass
import cn.usee.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FileFormatEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.ml.linalg.Vectors

/**
  * 基于word2vec相似度的相似影片推荐
  * Updated by Edison_Zhou on 2019/2/28.
  */
object SimilarWord2Vec extends BaseClass {
  def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._
    TransformUDF.registerUDFSS

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {
      //读入节目对应的Word2Vec数据
      val word2VecPath: HdfsPath = new HdfsPath(s"/ai/data/medusa/base/word2vec/item/Latest/${contentType}Features.txt", FileFormatEnum.TEXT)
      val word2VecSidDF = DataReader.read(word2VecPath)
        .rdd.map(line => line.getString(0).split(",")).
        map(e => (e(0), e.takeRight(128))).
        map(e => (e._1, Vectors.dense(e._2.map(x => x.toDouble))))
        .toDF("sid", "vector")

      //转虚拟sid的逻辑在usee业务中暂时不用
      //val word2VecVSidDF = BizUtils.transferToVirtualSid(word2VecSidDF, "sid")

      //有效影片的数据
      val validSidDF = BizUtils.getAvailableVideo(contentType)

      //数据的有效性检查
      val validAlg: ValidationCheckAlgorithm = new ValidationCheckAlgorithm()
      val validPara = validAlg.getParameters.asInstanceOf[ValidationCheckParameters]
      validPara.userOrItem = "item"
      val validDataMap = Map(validAlg.INPUT_DATA_KEY -> word2VecSidDF, validAlg.INPUT_CHECKLIST_KEY -> validSidDF)
      validAlg.initInputData(validDataMap)
      validAlg.run()
      val validTagSidDF = validAlg.getOutputModel.asInstanceOf[ValidationCheckModel].checkedData

      //相似度计算
      val similarAlg: SimilarVectorAlgorithm = new SimilarVectorAlgorithm()
      val similarPara: SimilarVectorParameters = similarAlg.getParameters.asInstanceOf[SimilarVectorParameters]
      similarPara.isSparse = false
      similarPara.topN = 60
      val similarDataMap = Map(similarAlg.INPUT_DATA_KEY -> validTagSidDF)
      similarAlg.initInputData(similarDataMap)
      similarAlg.run()
      //结果输出到HDFS
      similarAlg.getOutputModel.output("similarWord2Vec/" + contentType)
    })
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.uSee

}
