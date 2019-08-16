package com.zytc.yc.spark.config

/**
  * 全局配置
  */
object GlobalConfig {

    /**
      * 违规阀值(后期改成从mysql中获取)
      */
    var threshold: Map[String, Int] = Map(
        /**
          * 登录日志
          */
        //一天内登录失败次数
        "DayLoginFail" -> 10,
        //连续失败次数
        "FreqFail" -> 5,
        //一小时登录失败次数
        "HourLoginFail" -> 8,
        //十分钟登录次数
        "MinLogFail" -> 8,

        /**
          * 操作日志
          */
        // 查询/导出数据量异常
        "affectAbnormal" -> 100000,
        //查询/导出次数异常
        "countSelectAbnormal" -> 200,
        //次数查询/导出次数异常
        "shortTimeAbnoraml" -> 10000
    )
}
