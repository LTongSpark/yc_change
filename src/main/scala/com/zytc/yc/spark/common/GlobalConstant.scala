package com.zytc.yc.spark.common

/**
  * Author:   LTong
  * Date:     2019-04-28 12:36
  */
object GlobalConstant {

    /**
      * kafka相关
      */

    val KAFKA_BATCH_PERIOD = 1
    val KAFKA_TOPIC = "ycdata"
    val KAFKA_BROKER = "172.20.16.242:9191,172.20.16.243;9192,172.20.16.244:9193"

    /**
      * mysql相关
      */

    val MYSQL_URL = "jdbc:mysql://172.20.16.245:3306/ycdata"
    val MYSQL_USERNAME = "sjaq"
    val MYSQL_PASSWORD = "u1n2@yc"

    /**
      * 时间相关
      */

    val OUTPUT_TIME = "201811"
}
