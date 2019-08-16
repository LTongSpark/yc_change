package com.zytc.yc.spark.Repository

/**
  * 违规仓储
  */
object ViolationsRepository {

    /**
      * 违规仓储实例
      * 格式:Map("分析程序名称" -> Map("用户ID" -> Tuple(累计操作次数,最后一条记录的时间,标记)))
      */
    var Repository: Map[String, Map[String, Tuple3[Int, String, String]]] = Map(
        //操作仓储
        "affectAbnormal" -> Map(),
        "countSelectAbnormal" -> Map(),
        "ShortTimrAbnormal" -> Map(),
        //登录仓库
        "DayLoginFail" -> Map(),
        "FreqFail" -> Map(),
        "HourLoginFail" -> Map(),
        "MinLogFail" -> Map()
    )
}
