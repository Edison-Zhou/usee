package cn.usee.biz.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{EnvEnum, FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path._
import cn.moretv.doraemon.common.util.{ArrayUtils, DateUtils, TransformUtils}
import cn.moretv.doraemon.data.writer._
import cn.usee.biz.constant.PathConstants
import cn.whaley.sdk.dataOps.HDFSOps
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.json.JSONObject

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @author cheng.huan
  * @since 2019/03/04
  */
object BizUtils {

  /**
    * 过滤地域屏蔽
    * 要求输入的DataFrame中包含uid，sid这两列
    *
    * @param recommend  格式为：DataFrame[Row(uid,sid, 其他列....)],recommend可能包含不止uid,sid的列
    * @return DataFrame  recommend的子集
    */
  def filterRiskFlag(recommend: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    //获取用户风险等级
    val userRiskPath = new HivePath("select a.user_id, b.dangerous_level " +
      "from dw_dimensions.dim_medusa_terminal_user a left join dw_dimensions.dim_web_location b " +
      "on a.web_location_sk = b.web_location_sk " +
      "where a.dim_invalid_time is null and b.dangerous_level > 0")
    val userRisk = DataReader.read(userRiskPath).map(e=>(TransformUtils.calcLongUserId(e.getString(0)),e.getInt(1))).toDF("uid","userRisk")
    getDataFrameInfo(userRisk,"userRisk")

    //获取视频风险等级
    val videoRiskPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
      Array("sid","risk_flag"),
      "((contentType in ('tv','zongyi','comic','kids','jilu') and videoType = 1) || " +
        " (contentType = 'movie' and videoType = 0)) " +
        " and type = 1 and status = 1 ")
    val videoRisk = DataReader.read(videoRiskPath).map(e=>(e.getString(0),e.getInt(1))).toDF("sid","videoRisk")
    getDataFrameInfo(videoRisk,"videoRisk")

    val finalRecommend=recommend.join(userRisk, recommend("uid") === userRisk("uid"), "left")
      .join(videoRisk, recommend("sid") === videoRisk("sid"), "left")
      .where(expr("if(userRisk is null,0,userRisk)+if(videoRisk is null,0,videoRisk) <= 2"))
      .select(recommend("*"))

    finalRecommend
  }

  /**
    * 读取moretv长视频有效节目
    *
    * @return DataFrame[sid]
    */
  def getValidLongVideoSid(): DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val videoDataPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-4", 3306,
          "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
          Array("sid", "episodeCount", "contentType"),
          "((contentType in ('tv','zongyi','comic','kids','jilu') and videoType = 1) || " +
            " (contentType = 'movie' and videoType = 0)) " +
            " and type = 1 and status = 1 ")
        val videoData = DataReader.read(videoDataPath).map(r=>(r.getString(0), r.getInt(1), r.getString(2))).
          filter(x => if (x._3.equals("kids")) x._2 > 0 else true)
          .map(e=> e._1).toDF("sid")
        videoData
  }


  def getDataFrameWithDataRange(path: String, numOfDays: Int): DataFrame = {
    val dateRange = new DateRange("yyyyMMdd",numOfDays)
    val hdfsPath = new HdfsPath(dateRange,path)
    println(s"path = $hdfsPath")
    DataReader.read(hdfsPath)
  }

  def getDataFrameNewest(path: String): DataFrame = {
    if (HDFSOps.existsFile(path + "/Latest")) {
      println(s"read $path" + "/Latest")
      DataReader.read(new HdfsPath(path+ "/Latest"))
    } else {
      require(HDFSOps.existsFile(path + "/BackUp"), "The backUp result doesn't exist")
      DataReader.read(new HdfsPath(path+ "/BackUp"))
    }
  }

  /**
    *
    * 将uid,sid按照uid进行group by，获得sid的数组，然后随机打乱数组顺序
    *
    * @param dataFrame DataFrame
    * @param numOfRecommend  取多少sid
    * @param operation  数组操作方式
    * @return DataFrame
    */
  def getUidSidDataFrame(dataFrame: DataFrame,numOfRecommend:Int,operation:String): DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    var df:DataFrame=null
    val rdd=dataFrame.rdd.
      map(r => (r.getLong(0), r.getString(1))).
      groupByKey().map(e => (e._1, e._2.toArray))

    if(operation.equalsIgnoreCase(Constants.ARRAY_OPERATION_TAKE)){
      df=rdd.map(e => (e._1, e._2.take(numOfRecommend))).flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid")
    }else if(operation.equalsIgnoreCase(Constants.ARRAY_OPERATION_RANDOM)){
      df=rdd.map(e => (e._1, ArrayUtils.randomArray(e._2.toArray))).flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid")
    }else if(operation.equalsIgnoreCase(Constants.ARRAY_OPERATION_TAKE_AND_RANDOM)){
      df=rdd.map(e => (e._1, ArrayUtils.takeThenRandom(e._2.toArray,numOfRecommend))).flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid")
    }
    df
  }

  /**
    *
    * 将uid,sid按照uid进行group by，获得sid的数组，然后随机打乱数组顺序
    *
    * @param dataFrame DataFrame
    * @param numOfRecommend  取多少sid
    * @param operation  数组操作方式
    * @param min 用户对应的sid大于min才保留结果
    * @return DataFrame
    */
  def getUidSidDataFrame(dataFrame: DataFrame,numOfRecommend:Int,operation:String,min:Int): DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    var df:DataFrame=null

    val rdd=dataFrame.rdd.
      map(r => (r.getLong(0), r.getString(1))).
      groupByKey().filter(e=>e._2.size>=min).map(e => (e._1, e._2.toArray))

    if(operation.equalsIgnoreCase(Constants.ARRAY_OPERATION_TAKE)){
      df=rdd.map(e => (e._1, e._2.take(numOfRecommend))).flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid")
    }else if(operation.equalsIgnoreCase(Constants.ARRAY_OPERATION_RANDOM)){
      df=rdd.map(e => (e._1, ArrayUtils.randomArray(e._2.toArray))).flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid")
    }else if(operation.equalsIgnoreCase(Constants.ARRAY_OPERATION_TAKE_AND_RANDOM)){
      df=rdd.map(e => (e._1, ArrayUtils.takeThenRandom(e._2.toArray,numOfRecommend))).flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid")
    }
    df
  }

  /**
    *
    * 将uid,sid按照uid进行group by，获得sid的数组，然后随机打乱数组顺序
    *
    * @param base DataFrame
    * @param whiteOrBlack  黑名单或白名单
    * @param operation  left or right
    * @param blackOrWhite  过滤方式
    * @return DataFrame
    */
  def uidSidFilter(base: DataFrame,whiteOrBlack: DataFrame,operation:String,blackOrWhite:String): DataFrame = {
    var df:DataFrame=null
    if(operation.equalsIgnoreCase("left") && blackOrWhite.equalsIgnoreCase("black")){
      df=base.as("a").join(whiteOrBlack.as("b"), expr("a.uid = b.uid") && expr(s"a.sid = b.sid"), operation)
        .where("b.uid is null and b.sid is null")
        .selectExpr("a.*")
    }
    df
  }



  /**
    *
    * 获取节目会员正片
    *
    * @param riskFlag 输入riskFlag，如果riskFlag为大于-1的值，则表示取特定风险等级的vip影片。
    */
  def getVipSid(riskFlag:Int) : DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val videoDataPath: MysqlPath = new MysqlPath("bigtest-cmpt-129-204", 3306,
      "gemini_tvservice", "mtv_program_album", "root", "root1234",
      Array("sid", "episodeCount", "content_type", "tags", "risk_flag", "vip_type"),
      "content_type in ('tv','movie','zongyi','comic','kids','jilu') " +
        "and feature_type = 1 and status = 1 and vip_type=1 ")
    val videoData = DataReader.read(videoDataPath)
    val rddFromMysql=videoData.rdd.map(r=>(r.getString(0), r.getInt(1), r.getString(2), r.getString(3), r.getInt(4), r.getInt(5))).
      filter(x => if (x._3.equals("kids")) x._2 > 0 else true).
      map(x => (x._1, (x._3, x._4, x._5, x._6)))
    //RDD[(sid, (contentType, tags, risk_flag, supply_type))]
    if(riskFlag > -1){
      rddFromMysql.filter(e => e._2._3 == riskFlag)
        .map(e => e._1).toDF("sid")
    }else{
      rddFromMysql.map(e => e._1).toDF("sid")
    }
  }


  def rowNumber(df:DataFrame,partitionByName:String,orderByName:String,takeNumber:Int,isDropOrderByName:Boolean): DataFrame ={
      val dropColumns = new ArrayBuffer[String]()
      dropColumns+="temp"
      if(isDropOrderByName){
        dropColumns+=orderByName
      }
      df.withColumn("temp", row_number.over(Window.partitionBy(partitionByName).orderBy(col(orderByName).desc)))
      .filter("temp < "+takeNumber).drop(dropColumns.toSeq:_*)
  }

  def getDataFrameInfo(df: DataFrame,name:String): Unit = {
    if(df.columns.contains("uid") && df.columns.contains("sid"))
      println(s"$name.groupByUid.count():"+df.select("uid", "sid").groupBy("uid").agg(collect_list("sid")).toDF("uid", "recommend").count())
    println(s"$name.count():"+df.count())
    println(s"$name.printSchema:")
    df.printSchema()
    df.show(10, false)
  }



  /**
    * 推荐结果插入kafka
    *
    * @param servicePrefix 业务前缀
    * @param df     推荐结果
    * @param alg    算法名称
    * @param kafkaTopic    kafka topic
    */
  def recommend2Kafka4Couchbase(servicePrefix: String,df:DataFrame,alg: String, kafkaTopic: String): Unit = {
    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = servicePrefix
    param.extraValueMap = Map("date" -> DateUtils.getTimeStamp, "alg" -> alg)
    val path: Path = CouchbasePath(kafkaTopic)
    val dataWriter: DataWriter = new DataWriter2Kafka
    val result=df.groupBy("uid").agg(collect_list("sid")).toDF("uid","id")
    val dataSource = DataPack.pack(result, param)
    dataWriter.write(dataSource, path)
  }

  /**
    * 默认推荐插入kafka
    *
    * @param servicePrefix 业务前缀
    * @param recommend     推荐结果
    * @param kafkaTopic    kafka topic
    */
  def defaultRecommend2Kafka4Couchbase(servicePrefix: String,
                                       recommend: Array[String],
                                       alg: String,
                                       kafkaTopic: String,numOfRecommend2Kafka:Int): Unit = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = servicePrefix
    param.extraValueMap = Map("date" -> DateUtils.getTimeStamp, "alg" -> alg)
    val path: Path = CouchbasePath(kafkaTopic, 1728000)
    val dataWriter: DataWriter = new DataWriter2Kafka
    val recommendSid = recommend.take(numOfRecommend2Kafka)
    val list = List(("default", recommendSid))
    val dataSource = DataPack.pack(list.toDF("default", "id"), param)
    dataSource.collect().foreach(println)
    for (i <- 0 to 100){dataWriter.write(dataSource, path)}
  }

  /**用于读取出活跃&&筛选出活跃用户
 *
    * @return DataFrame[user]
    * */
  def readActiveUser(): DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val df = getDataFrameNewest(PathConstants.pathOfMoretvActiveUser)
    val rdd=df.rdd.map(r => (r.getLong(0), r.getInt(1), r.getInt(2), r.getInt(3), r.getInt(4)))
             .filter(e => (e._5 >= 1 || e._4 >= 1 || e._3 >= 1 || e._2 >= 1 )).map(e => (e._1))
    val finalDataFrame = rdd.toDF("user")
    finalDataFrame
  }

  /** 用于获取评分数据
    *
    * @return DataFrame[user,item,rating]
    * */
  def readUserScore(path: String, numOfDays: Int): DataFrame= {
    val endDate = DateUtils.todayWithDelimiter
    val startDate = DateUtils.farthestDayWithDelimiter(numOfDays)
    val df = DataReader.read(new HdfsPath(path))
    val result = df.filter(s"latest_optime >= '$startDate' and latest_optime <= '$endDate'").
      selectExpr("userid as user", "sid_or_subject_code as item","score as rating")
    result
  }



  /** 获取“编辑精选”标签的电影，并赋予权重
    *
    * @return Map[String,Double]
    * */
  def sidFromEditor(weightOfSelectMovies:Double):Map[String,Double]={
    val tagDataPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_mapping", "bislave", "slave4bi@whaley", Array("sid"),
      "tag_id = 152063")
    val selectMovies = DataReader.read(tagDataPath)
    BizUtils.getDataFrameInfo(selectMovies,"selectMovies")
    val weightVideos = selectMovies.rdd.map(r => (r.getString(0)))
      .map(e => (e, weightOfSelectMovies))
      .collectAsMap().toMap
    weightVideos
  }

  /**
    * 获取短视频专题列表
    *
    * @return DataFrame[(code, copyright)]
    */
  def getHotSubjects: DataFrame = {
    val subjectPath = new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_subject", "bislave", "slave4bi@whaley", Array("id", "code", "copyright"),
      "status = 1 and recommend = 1 and " +
    " (code REGEXP 'hot([0-9]+)$' || " +
      " code REGEXP 'tv([0-9]+)$' || " +
      " code REGEXP 'mv([0-9]+)$' || " +
      " code REGEXP 'jilu([0-9]+)$' || " +
      " code REGEXP 'kids([0-9]+)$' || " +
      " code REGEXP 'game([0-9]+)$' || " +
      " code REGEXP 'comic([0-9]+)$' || " +
      " code REGEXP 'movie([0-9]+)$') ")
    val subjectPagePath = new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_subject_page", "bislave", "slave4bi@whaley", Array("subject_id"),
      "mode in (2,4,7,8)")
    val subjectDF = DataReader.read(subjectPath)
    val subjectPageDF = DataReader.read(subjectPagePath)

    subjectDF.join(subjectPageDF, expr("id = subject_id")).select("code", "copyright")
  }

  def getMysqlPath(tag: String): Path = {

    tag match {
      case "movie_editor_recommend" =>
        new MysqlPath("bigdata-appsvr-130-6", 3306,
          "europa", "tag_program_mapping", "bislave", "slave4bi@whaley", "id",
          Array("sid", "'movie' as cluster"),
          "tag_id = 152063 and status = 1") //筛选电影的编辑精选

      case "movie_all_sid" =>
        new MysqlPath("bigdata-appsvr-130-2", 3306,
          "mtv_cms", "mtv_basecontent", "bislave", "slave4bi@whaley", "id",
          Array("sid", "content_type as cluster"),
          "content_type = 'movie'")
      //少儿教育测试环境数据库地址（10.19.143.44）
      case "kids_edu_small_window_sid" =>
        new MysqlPath("10.19.143.44", 3306,
          "tvservice", "mtv_program", "readonly", "readonly", "id",
          Array("sid"),
          "videoType = 2 and parentId in (10051811,10878971,32831411,54386001,46804461,61170011,1004410981,2012763581,2010102989,2009124282,2009636618,2010219015,2010590351,2010681346,2012209401,2008760570,2008836613,2008837078,2008843201,2008985201,4546180)")
      //测试ip 10.19.23.91 用户：cmsUser, 密码：cmsUser@654321, db: gemini_cms
      //现网 "10.10.195.241", 3306, "gemini_tvservice", "readonly", "readonly"
      case "long_valid_sid" =>
        new MysqlPath("10.10.195.241", 3306,
          "gemini_tvservice", "mtv_program_album", "readonly", "readonly", "id",
          Array("sid","content_type", "title", "risk_flag"),
          "sid is not null and title is not null and status = 1 " +
            "and content_type in ('tv', 'zongyi', 'jilu', 'kids', 'comic', 'movie') and feature_type = 1")

      case "short_valid_sid" =>
        new MysqlPath("10.10.195.241", 3306,
          "gemini_tvservice", "mtv_program_album", "readonly", "readonly", "id",
          Array("sid","content_type", "title", "risk_flag","update_time"),
          "sid is not null and title is not null and status = 1 and risk_flag is not null and update_time is not null " +
            "and content_type in ('hot', 'game', 'sports', 'mv')")

      case "valid_subject" =>
        new MysqlPath("10.10.195.241", 3306,
          "gemini_tvservice", "mtv_subject", "readonly", "readonly", "id",
          Array("code", "title"),
          "code is not null and title is not null and status = 1")

      case "valid_person" =>
        new MysqlPath("10.10.195.241", 3306,
          "gemini_tvservice", "mtv_person", "readonly", "readonly", "id",
          Array("sid", "name"),
          "sid is not null and name is not null and status = 1")

      case "movie_valid_sid" => validSidPath("movie")
      case "yueting_movie_valid_sid" => validSidPath("yueting_movie")
      case "tv_valid_sid" => validSidPath("tv")
      case "yueting_tv_valid_sid" => validSidPath("yueting_tv")
      case "jilu_valid_sid" => validSidPath("jilu")
      case "kids_valid_sid" => validSidPath("kids")
      case "zongyi_valid_sid" => validSidPath("zongyi")
      case "comic_valid_sid" => validSidPath("comic")
      case _ => null
    }
  }

  def getMysqlPath4Test(tag: String): Path = {
    tag match {
      case "long_valid_sid" =>
        new MysqlPath("10.19.23.91", 3306,
          "gemini_tvservice", "mtv_program_album", "readonly", "readonly", "id",
          Array("sid","content_type", "title", "risk_flag"),
          "sid is not null and title is not null and status = 1 " +
            "and content_type in ('tv', 'zongyi', 'jilu', 'kids', 'comic', 'movie') and feature_type = 1")

      case "short_valid_sid" =>
        new MysqlPath("10.19.23.91", 3306,
          "gemini_tvservice", "mtv_program_album", "readonly", "readonly", "id",
          Array("sid","content_type", "title", "risk_flag","update_time"),
          "sid is not null and title is not null and status = 1 and risk_flag is not null and update_time is not null " +
            "and content_type in ('hot', 'game', 'sports', 'mv')")

      case "valid_subject" =>
        new MysqlPath("10.19.23.91", 3306,
          "gemini_tvservice", "mtv_subject", "readonly", "readonly", "id",
          Array("code", "title"),
          "code is not null and title is not null and status = 1")

      case "valid_person" =>
        new MysqlPath("10.19.23.91", 3306,
          "gemini_tvservice", "mtv_person", "readonly", "readonly", "id",
          Array("sid", "name"),
          "sid is not null and name is not null and status = 1")

      case _ => null
    }
  }

  private def validSidPath(contentType: String): MysqlPath = {
    new MysqlPath("10.10.195.241", 3306,
      "gemini_tvservice", "mtv_program_album", "readonly", "readonly",
      Array("sid", "content_type"),
    contentType match {
      case _ => "sid is not null and title is not null " +
        s" and content_type = '$contentType' and status = 1 and feature_type = 1"
    })
  }

  /*private def validSidPath(contentType: String): HivePath ={
    val sql = contentType match {
      case "movie" =>
        s"""
           | select sid from `ods_view`.`db_snapshot_mysql_medusa_mtv_program` where key_day='latest' and key_hour='latest'
           | and sid is not null and title is not null
           | and contentType = '$contentType' and status = 1 and type = 1
         """.stripMargin
      case "yueting_movie" =>
        s"""
           | select sid from `ods_view`.`db_snapshot_mysql_medusa_mtv_program` where key_day='latest' and key_hour='latest'
           | and sid is not null and title is not null
           | and contentType = 'movie' and status = 1 and type = 1 and copyright_code='sohu'
         """.stripMargin
      case "yueting_tv" => s"""
                   | select sid from `ods_view`.`db_snapshot_mysql_medusa_mtv_program` where key_day='latest' and key_hour='latest'
                   | and sid is not null and title is not null
                   |  and contentType = 'tv' and status = 1 and type = 1 and videoType = 1  and copyright_code='sohu'
         """.stripMargin
      case "all" => s"""
                   | select sid from `ods_view`.`db_snapshot_mysql_medusa_mtv_program` where key_day='latest' and key_hour='latest'
                   | and sid is not null and title is not null and status = 1 and type = 1
                   |  and (
                   |   (contentType = 'movie' and  videoType = 0 )   or
                   |   (contentType in('tv','zongyi','comic','kids','jilu') and videoType = 1 )
                   |  )
         """.stripMargin
      case _ => s"""
                   | select sid from `ods_view`.`db_snapshot_mysql_medusa_mtv_program` where key_day='latest' and key_hour='latest'
                   | and sid is not null and title is not null
                   |  and contentType = '$contentType' and status = 1 and type = 1 and videoType = 1
         """.stripMargin

    }

    new HivePath(sql)
  }*/



  def getHdfsPathForRead(bizName: String)(implicit productLine: ProductLineEnum.Value, env: EnvEnum.Value): HdfsPath = {
    val basePath = env match {
      case EnvEnum.PRO => Constants.OUTPUT_PATH_BASE
      case EnvEnum.TEST => Constants.OUTPUT_PATH_BASE_DEBUG + EnvEnum.TEST.toString.toLowerCase + "/"
      case other: EnvEnum.Value => Constants.OUTPUT_PATH_BASE_DEBUG + other.toString.toLowerCase + "/"
      case _ => throw new RuntimeException("不合法的环境类型")
    }
    val path = basePath + productLine.toString.toLowerCase + "/" + bizName + "/Latest"
    new HdfsPath(path)
  }

  def outputWrite(df: DataFrame,bizName: String)(implicit productLine: ProductLineEnum.Value, env: EnvEnum.Value): Unit = {
    val basePath = env match {
      case EnvEnum.PRO => Constants.OUTPUT_PATH_BASE
      case EnvEnum.TEST => Constants.OUTPUT_PATH_BASE_DEBUG + EnvEnum.TEST.toString.toLowerCase + "/"
      case other: EnvEnum.Value => Constants.OUTPUT_PATH_BASE_DEBUG + other.toString.toLowerCase + "/"
      case _ => throw new RuntimeException("不合法的环境类型")
    }
    val path = basePath + productLine.toString.toLowerCase + "/" + bizName
    println(s"path = $path")
    new DataWriter2Hdfs().write(df,new HdfsPath(path))
  }

  /**
    * 获取sid对应的专题号
    *
    * @return
    */
  def getVideoId2subject: DataFrame = {
    val path = new MysqlPath("bigdata-appsvr-130-4", 3306, "tvservice", "mtv_subjectItem",
      "bislave", "slave4bi@whaley", Array("item_sid", "subject_code", "item_contentType"),
      "item_sid is not NULL and subject_code is not null and item_contentType is not null and status = 1")

    DataReader.read(path)
  }

  /**
    * 获取可用专题类型和专题code
    *
    * @return
    */
  def getAvailableSubject: DataFrame = {
    val subjectPath = new MysqlPath("bigdata-appsvr-130-4", 3306, "tvservice", "mtv_subject",
      "bislave", "slave4bi@whaley", Array("id", "code", "content_type"),
    "id is not null and code is not null and content_type is not null and status = 1 and recommend = 1")
    val subjectPagePath = new MysqlPath("bigdata-appsvr-130-4", 3306, "tvservice", "mtv_subject_page",
      "bislave", "slave4bi@whaley", Array("subject_id"),
      "mode not in (2,3,4,5,6,7,8,10)")

    val subjectDF = DataReader.read(subjectPath)
    val subjectPageDF = DataReader.read(subjectPagePath)

    subjectDF.join(subjectPageDF, expr("id = subject_id"), "left").select("content_type", "code")
  }

  /**
    * 根据视频类型获取可用的视频信息
    *
    * @return
    */
  def getAvailableVideo(contentType: String)(implicit sqlContext: SQLContext): DataFrame = {
    val path = validSidPath(contentType)
    DataReader.read(path)

    //转虚拟id，行数减少
//    val sidRel = readVirtualSidRelation()
//    validSid.as("a").join(sidRel.as("b"), expr("a.sid = b.sid"), "leftouter")
//      .selectExpr("case when b.virtual_sid is not null then b.virtual_sid else a.sid end as sid")
//      .distinct()
  }

  def readVirtualSidRelation()(implicit sqlContext: SQLContext): DataFrame = {
    val sql = "select virtual_sid, sid from ods_view.db_snapshot_mysql_medusa_mtv_virtual_content_rel " +
      "where key_day = 'latest' and key_hour = 'latest' and status = 1"
    sqlContext.sql(sql).dropDuplicates("sid")
  }

  //转成虚拟id，删除重复的行
  def transferToVirtualSid(input: DataFrame, sidColumnName: String)(implicit sqlContext: SQLContext): DataFrame = {
    val sidRel = readVirtualSidRelation()
    input.as("a").join(sidRel.as("b"), expr(s"a.$sidColumnName = b.sid"), "leftouter").drop(expr("b.sid"))
      .withColumn(sidColumnName, expr(s"case when b.virtual_sid is not null then b.virtual_sid else a.$sidColumnName end"))
      .drop(expr("b.virtual_sid"))
      .dropDuplicates(sidColumnName)
  }

  //把评分中映射到同一个虚拟id的sid整合起来
  def scoreMaxToVirtualSid(input: DataFrame, sidColumnName: String)(implicit sqlContext: SQLContext): DataFrame = {
    val sidRel = readVirtualSidRelation()
    sidRel.printSchema()
    sidRel.show(10, false)
    input.as("a").join(sidRel.as("b"), expr(s"a.$sidColumnName = b.sid"), "leftouter").drop(expr("b.sid"))
      .withColumn(sidColumnName, expr(s"case when b.virtual_sid is not null then b.virtual_sid else a.$sidColumnName end"))
      .drop(expr("b.virtual_sid"))
      .groupBy("userid", s"$sidColumnName", "optime", "content_type", "episodesid", "episode_index").agg(max("score").as("score"))
      .dropDuplicates("userid", sidColumnName)
  }

  /**
    * 从评分矩阵中获取用户近N天的日志数据
    *
    */
  /*def getUserInfoFromScoreMatrix(numOfDays: Int,
                                 score: Double = 0.6,
                                 contentType: String = "movie")(implicit ss: SparkSession) = {

    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -numOfDays)
    val detailFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date =  detailFormat.format(calendar.getTime)

    val pathOfScore=PathConstants.pathOfScore.replace("contentTypeVar",contentType)
    val data = ss.read.parquet(pathOfScore).
      filter(s"latest_optime > '${date}'").
      selectExpr("userid", "sid_or_subject_code", "score").
      groupBy("userid", "sid_or_subject_code").agg(Map("score" -> "sum")).
      withColumnRenamed("sum(score)", "score").filter(s"score > ${score} ")

    import ss.implicits._
    data.selectExpr("userid", "sid_or_subject_code", "score").rdd.
      map(r => (r.getLong(0), r.getString(1).toInt, r.getDouble(2))).
      toDF("uid", "sid", "score")
  }*/


  /**
    * 从评分矩阵中,获取近N天的活跃用户
    * @param numOfDays 获取数据的天数
    * @param score 评分下限
    * @param contentType 视频类型
    */
  def getActiveUserFromScoreMatrix(numOfDays: Int,
                                   score: Double = 0.6,
                                   contentType: String = "movie"):DataFrame= {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -numOfDays)
    val detailFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date =  detailFormat.format(calendar.getTime)

    val pathOfScore=PathConstants.pathOfScore.replace("contentTypeVar",contentType)
    val data = DataReader.read(new HdfsPath(pathOfScore)).
      filter(s"latest_optime > '${date}'").
      selectExpr("userid", "sid_or_subject_code", "score").
      groupBy("userid", "sid_or_subject_code").agg(Map("score" -> "sum")).
      withColumnRenamed("sum(score)", "score").filter(s"score > ${score} ")
    val result=data.select("userid").distinct().withColumnRenamed("userid","uid")
    result
  }

  /**
    * 从评分矩阵中,获取所有频道近N天的活跃用户
    * @param numOfDays 获取数据的天数
    * @param score 评分下限
    */
  def getActiveUserFromScoreMatrixForAllContentType(numOfDays: Int,
                                   score: Double = 0.6):DataFrame= {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -numOfDays)
    val detailFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date =  detailFormat.format(calendar.getTime)
    val data = DataReader.read(new HdfsPath(PathConstants.pathOfScoreAllContentType)).
      filter(s"latest_optime > '${date}'").
      selectExpr("userid", "sid_or_subject_code", "score").
      groupBy("userid", "sid_or_subject_code").agg(Map("score" -> "sum")).
      withColumnRenamed("sum(score)", "score").filter(s"score > ${score} ")
    val result=data.select("userid").distinct().withColumnRenamed("userid","uid")
    result
  }

  /**
    * 从评分矩阵中,获得用户在某一个contentType中，最近numberOfDays天的观看sid历史
    *
    * @param contentType 视频类型
    * @param numberOfDays 起始时间
    * @return result 用户与用户观看过的sid的set
    */
  def getUserWatchedSidByContentType(contentType: String, numberOfDays: Int):RDD[(Long,Set[String])]= {
    val endDateWatched = DateUtils.todayWithDelimiter
    val startDateWatched = DateUtils.farthestDayWithDelimiter(numberOfDays)
    val path=PathConstants.pathOfScore.replace("contentTypeVar",contentType)
    val result = DataReader.read(new HdfsPath(path)).
    filter(s"latest_optime >= '$startDateWatched' and latest_optime <= '$endDateWatched'").
    selectExpr("userid", "sid_or_subject_code").
    rdd.map(r => (r.getLong(0), r.getString(1))).
    groupByKey().map(r => (r._1, r._2.toSet))
    result
  }

  /**
    * 从评分矩阵中,获得用户在所有contentType中，最近numberOfDays天的观看sid历史
    *
    * @param numberOfDays 起始时间
    * @return result 用户与用户观看过的sid的set
    */
  def getUserWatchedSidForAllContentType(numberOfDays: Int):RDD[(Long,Set[String])]= {
    val endDateWatched = DateUtils.todayWithDelimiter
    val startDateWatched = DateUtils.farthestDayWithDelimiter(numberOfDays)
    val result = DataReader.read(new HdfsPath(PathConstants.pathOfScoreAllContentType)).
      filter(s"latest_optime >= '$startDateWatched' and latest_optime <= '$endDateWatched'").
      selectExpr("userid", "sid_or_subject_code").
      rdd.map(r => (r.getLong(0), r.getString(1))).
      groupByKey().map(r => (r._1, r._2.toSet))
    result
  }

  /**
    * 获取猜你喜欢算法标识
    *
    * @param uid 用户id
    * @return 返回alg标识
    */
  def getAlg(uid: Long) :String={
    // ab测试（moretv的）url接口
    val url = s"http://10.19.43.203:3456/config/abTest?userId=${uid}&version=moretv"
    val json = new JSONObject(scala.io.Source.fromURL(url).mkString)
    val alg = json.getJSONObject("abTest").getJSONObject("guessulike").getString("alg")
    alg
  }


  /**
    * 推荐结果插入kafka
    *
    * @param servicePrefix 业务前缀
    * @param df     推荐结果
    * @param alg    算法名称
    * @param kafkaTopic    kafka topic
    */
  def recommend2Kafka4Couchbasex(servicePrefix: String,df:DataFrame,alg: String, kafkaTopic: String): Unit = {
    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = servicePrefix
    param.extraValueMap = Map("date" -> DateUtils.getTimeStamp, "alg" -> alg)
    val path: Path = CouchbasePath(kafkaTopic)
    val dataWriter: DataWriter = new DataWriter2Kafka
    val result=df.groupBy("uid").agg(collect_list("sid")).toDF("uid","id")
    val dataSource = DataPack.pack(result, param)
    dataWriter.write(dataSource, path)
  }

  /**
    * 推荐结果插入kafka for 站点树重排序,用户到用户组的对应关系
    *
    * @param servicePrefix 业务前缀
    * @param df     推荐结果
    * @param kafkaTopic    kafka topic
    *
    *
    * o:msru:1000002525747273722
    *
    * {
    * "groupId": "113"
    * }
    */
  def recommend2KafkaTabReorder(servicePrefix: String,df:DataFrame, kafkaTopic: String): Unit = {
    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = servicePrefix
    val path: Path = CouchbasePath(kafkaTopic)
    val dataWriter: DataWriter = new DataWriter2Kafka
    val dataSource = DataPack.pack(df, param)
    dataWriter.write(dataSource, path)
  }

  /**
    * 推荐结果插入kafka for 站点树重排序，特定频道特定code特定用户组对应的节目
    *
    * @param servicePrefix 业务前缀
    * @param df     推荐结果
    * @param kafkaTopic    kafka topic
    * @param alg   算法标识
    *
    * o:mt:movie_1_movie_tag_dongzuo_4_0_3000
    *
    * {
    *    "alg": "reorderALS",
    *    "id": [
    *            1000043194,
    *            52825661
    *          ]
    * }
    */
  def recommend2KafkaTabReorder(servicePrefix: String,df:DataFrame, kafkaTopic: String,alg:String): Unit = {
    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = servicePrefix
    param.extraValueMap = Map("alg" -> alg)
    val path: Path = CouchbasePath(kafkaTopic)
    val dataWriter: DataWriter = new DataWriter2Kafka
    val dataSource = DataPack.pack(df, param)
    dataWriter.write(dataSource, path)
  }

  /**
    * 获取基于豆瓣评分的最热榜DF [sid, content_type, score]
    * @param contentType 视频类型
    * @param riskFlag 指定风险等级
    * @return
    */
  def hotDFBasedOnScore(contentType:String, riskFlag:Int):DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    val hotDF = DataReader.read(new MysqlPath("bigdata-appsvr-130-6", 3306, "europa",
      "tag_youku_program", "bislave", "slave4bi@whaley", Array("sid", "content_type", "youku_rating"),
      "content_type in ('movie', 'tv', 'kids', 'comic', 'jilu', 'zongyi') " +
        s"and youku_rating >= 8.0 and risk_flag = $riskFlag and status = 1 and type = 1"))
      .selectExpr("sid", "content_type", "youku_rating")
    contentType match {
      case "all" => hotDF
      case _ => hotDF.where(s"content_type ='$contentType'")
    }
  }

  def vipHotDFBasedOnScore(contentType:String, riskFlag:Int):DataFrame = {
    val vipSidDF = DataReader.read(new MysqlPath("10.10.195.241", 3306, "gemini_tvservice",
      "mtv_program_album", "readonly", "readonly", Array("sid", "vip_type"),
      s"vip_type = 1 and risk_flag = $riskFlag and status = 1 and feature_type = 1"))
    hotDFBasedOnScore(contentType, riskFlag).join(vipSidDF, "sid")
  }

  /**
    * 获取热门榜单的列表/数组
    * @param contentType 视频类型
    * @param riskFlag 指定风险等级
    * @return
    */
  def getHotRankingList(contentType: String, riskFlag:Int): Array[String] = {
    val hotTypeDF = hotDFBasedOnScore(contentType, riskFlag)
    BizUtils.getDataFrameInfo(hotTypeDF, "hotTypeDF")
    hotTypeDF.rdd.map(r => r.getString(0)).collect()
  }

  /**
    * 获取最热榜中的vip节目
    * @param contentType 视频类型
    * @param riskFlag 指定的风险等级
    * @return
    */
  def getHotRankingVipList(contentType: String, riskFlag:Int): Array[String] = {
    val hotVipTypeDF = vipHotDFBasedOnScore(contentType, riskFlag)
    BizUtils.getDataFrameInfo(hotVipTypeDF, "hotVipTypeDF")
    hotVipTypeDF.rdd.map(r => r.getString(0)).collect()
  }


  /**
    * 获取评分数据
    * @param scoreBaseDF 评分数据
    * @param contentType 数据类型
    * @param topN
    * @return
    */
  def getScoreList(scoreBaseDF:DataFrame,contentType: String, topN: Int): Array[String] = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    val scoreDF = contentType match {
      case "all" => scoreBaseDF
      case _ => scoreBaseDF.where(s"content_type ='$contentType'")
    }
    val groupedDF = scoreDF.filter("score > 0.1").groupBy("sid_or_subject_code").count().sort($"count".desc)
    groupedDF.rdd.map(r => r.getString(0)).collect().take(topN)
  }

  /**
    * 获取搜索算法节目相关数据
    * @param ss
    * @param date
    * @return
    */
  def getSearchVideoData(ss:SparkSession,date:String): DataFrame ={
    ss.sql(
      s"""
         | select a.* from
         | (  select * from dws_medusa_bi.medusa_video_score
         |    where day_p = '$date'
         |     and  content_type in ('movie','tv','zongyi','comic','kids','jilu')
         | ) a
         | inner join
         | (
         |    select sid from `ods_view`.`db_snapshot_mysql_medusa_mtv_program`
         |    where key_day='latest' and key_hour='latest'
         |    and sid is not null and title is not null and status = 1 and type = 1
         |     and (
         |            (contentType = 'movie' and  videoType = 0 ) or
         |          (contentType in('tv','zongyi','comic','kids','jilu') and videoType = 1)
         |        )
         |  ) b
         | on a.sid = b.sid
       """.stripMargin)
  }




  /**
    * 4.0大首页打包：首页今日推荐+vip推荐+短视频专题推荐
    * @param portalDF
    * @param portalAlg
    * @param vipDF
    * @param vipAlg
    * @param hotDF
    * @param hotAlg
    * @return
    */
  def frontPageUnionPack(portalDF:DataFrame,
                         portalAlg:String,
                         vipDF:DataFrame,
                         vipAlg:String,
                         hotDF:DataFrame,
                         hotAlg:String):DataFrame = {
    val portalFoldDF = portalDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")
    val vipFoldDF = vipDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")
    val hotFoldDF = hotDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")

    BizUtils.getDataFrameInfo(portalFoldDF, "portalFoldDF")
    BizUtils.getDataFrameInfo(vipFoldDF, "vipFoldDF")
    BizUtils.getDataFrameInfo(hotFoldDF, "hotFoldDF")

    val dataPackParam1 = new DataPackParam
    dataPackParam1.format = FormatTypeEnum.KV
    dataPackParam1.extraValueMap = Map("alg" -> portalAlg)
    val portalPackDF = DataPack.pack(portalFoldDF, dataPackParam1).toDF("key", "portal")

    BizUtils.getDataFrameInfo(portalPackDF, "portalPackDF")

    val dataPackParam2 = new DataPackParam
    dataPackParam2.format = FormatTypeEnum.KV
    dataPackParam2.extraValueMap = Map("alg" -> vipAlg)
    val vipPackDF = DataPack.pack(vipFoldDF, dataPackParam2).toDF("key", "vip")

    BizUtils.getDataFrameInfo(vipPackDF, "vipPackDF")

    val dataPackParam3 = new DataPackParam
    dataPackParam3.format = FormatTypeEnum.KV
    dataPackParam3.extraValueMap = Map("alg" -> hotAlg)
    val hotPackDF = DataPack.pack(hotFoldDF, dataPackParam3).toDF("key", "hot")

    BizUtils.getDataFrameInfo(hotPackDF, "hotPackDF")

    val dataPackParam4 = new DataPackParam
    dataPackParam4.format = FormatTypeEnum.KV
    dataPackParam4.keyPrefix = "p:a:"
    dataPackParam4.extraValueMap = Map("date" -> DateUtils.getTimeStamp)
    val unionDF = portalPackDF.as("a").join(vipPackDF.as("b"), expr("a.key = b.key"), "full").join(hotPackDF.as("c"), expr("a.key = c.key"), "full")
      .selectExpr("case when a.key is not null then a.key when b.key is not null then b.key else c.key end as key", "a.portal as portal", "b.vip as vip", "c.hot as hot")

    DataPack.pack(unionDF, dataPackParam4)
  }

  /**
    * 详情页打包：相似影片+主题推荐+专题推荐
    * @param similarDF
    * @param similarAlg
    * @param themeDF
    * @param themeAlg
    * @param subjectDF
    * @param subjectAlg
    * @return
    */
  def detailPageUnionPack(hotDF:DataFrame,
                          hotAlg:String,
                          similarDF:DataFrame,
                          similarAlg:String,
                          themeDF:DataFrame,
                          themeAlg:String,
                          subjectDF:DataFrame,
                          subjectAlg:String):DataFrame = {



    val hotFoldDf = hotDF.groupBy("sid").agg(collect_list("item")).toDF("sid", "id")
      .selectExpr("cast(sid as string) as sid", "id")

    val similarFoldDf = similarDF.groupBy("sid").agg(collect_list("item")).toDF("sid", "id")
      .selectExpr("cast(sid as string) as sid", "id")

    val themeFoldDf = themeDF.groupBy("sid").agg(collect_list("item")).toDF("sid", "id")
      .selectExpr("cast(sid as string) as sid", "id")

    val subjectFoldDf = subjectDF.toDF("sid", "id").selectExpr("cast(sid as string) as sid", "id")

    val dataPackParam = new DataPackParam
    dataPackParam.format = FormatTypeEnum.KV
    dataPackParam.extraValueMap = Map("alg" -> similarAlg,"title" -> "热门推荐")
    val hotPackDf = DataPack.pack(hotFoldDf, dataPackParam).toDF("key", "detailHot")

    val dataPackParam1 = new DataPackParam
    dataPackParam1.format = FormatTypeEnum.KV
    dataPackParam1.extraValueMap = Map("alg" -> similarAlg,"title" -> "相似影片")
    val similarPackDf = DataPack.pack(similarFoldDf, dataPackParam).toDF("key", "similar")

    val dataPackParam2 = new DataPackParam
    dataPackParam2.format = FormatTypeEnum.KV
    dataPackParam2.extraValueMap = Map("alg" -> themeAlg, "title" -> "更多精彩")
    val themePackDf = DataPack.pack(themeFoldDf, dataPackParam2).toDF("key", "theme")

    val dataPackParam3 = new DataPackParam
    dataPackParam3.format = FormatTypeEnum.KV
    dataPackParam3.extraValueMap = Map("alg" -> subjectAlg, "title" -> "专题推荐")
    val subjectPackDf = DataPack.pack(subjectFoldDf, dataPackParam3).toDF("key", "subject")

    val dataPackParam4 = new DataPackParam
    dataPackParam4.format = FormatTypeEnum.KV
    dataPackParam4.extraValueMap = Map("date" -> new SimpleDateFormat("yyyyMMdd hh:mm").format(new Date()))

    val joinDf = similarPackDf.as("a").join(themePackDf.as("b"), expr("a.key = b.key"), "full")
      .join(hotPackDf.as("c"), expr("a.key = c.key or b.key=c.key"), "full")
      .join(subjectPackDf.as("d"), expr("a.key = d.key or b.key=d.key or c.key=d.key"), "full")
      .selectExpr("case when a.key is not null then a.key when b.key is not null then b.key when c.key is not null then c.key else d.key end as key",
        "a.similar", "b.theme", "c.detailHot","d.subject")
    DataPack.pack(joinDf, dataPackParam4)
  }



  final def getOutPutPath(env: EnvEnum.Value,productLine:String,bizName:String): String = {
    val basePath = env match {
      case EnvEnum.PRO => Constants.OUTPUT_PATH_BASE
      case EnvEnum.TEST => Constants.OUTPUT_PATH_BASE_DEBUG + EnvEnum.TEST.toString.toLowerCase + "/"
      case other: EnvEnum.Value => Constants.OUTPUT_PATH_BASE_DEBUG + other.toString.toLowerCase + "/"
      case _ => throw new RuntimeException("不合法的环境类型")
    }
    basePath + productLine + "/" + bizName
  }

  def save(): Unit ={

  }
}
