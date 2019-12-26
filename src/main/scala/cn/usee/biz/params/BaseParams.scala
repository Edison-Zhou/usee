package cn.usee.biz.params

import scopt.OptionParser


case class BaseParams(
                    env: String = null,
                    productLine: String = null
                     )

object BaseParams{
  lazy val parser: OptionParser[BaseParams] = new OptionParser[BaseParams]("ParamsParse"){
    override def errorOnUnknownArgument = false
    opt[String]("env").
      action((x, c) => c.copy(env = x))
    opt[String]("productLine").
      action((x, c) => c.copy(env = x))
  }

  def getParams(args: Array[String]): BaseParams = {
    var params: BaseParams = null
    parser.parse(args, BaseParams()) match {
      case Some(param) =>
        params = param
        params
      case None => {
        println("ERROR: BaseParams wrong input parameters")
        System.exit(1)
        params
      }
    }
  }
}