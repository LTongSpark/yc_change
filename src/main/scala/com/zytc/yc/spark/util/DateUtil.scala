package com.zytc.yc.spark.util

import java.time.LocalDateTime

import org.joda.time.DateTime

object DateUtil {
    /**
      * 获取前一个的月的日期
      * @return
      */
    def parseLocalTime(): String ={
        val localTime = LocalDateTime.now()
        val endTime = localTime.plusMonths(-1)
        endTime.getYear.toString + endTime.getMonth.getValue.toString
    }

    /**
      * 将时间转化为yyyyMMddHHmmss和yyyy-MM-dd HH:mm:ss
      * @param timestamp
      * @return
      */
    def DateTimeHH(timestamp: String) = {
        new DateTime(timestamp.toLong).toString("yyyyMMddHHmmss")
    }
    def parseDateTime(timestamp: String) = {
        new DateTime(timestamp.toLong).toString("yyyy-MM-dd HH:mm:ss")
    }


    /**
      * 获取当前时间至昨晚18:00的分钟数
      * @return
      */
    def getMinutesToYesterdayEighteen() : Int = {
        //计算当前时刻至昨晚
        val nowMinutes= LocalDateTime.now().getMinute
        val nowHour = LocalDateTime.now().getHour
        nowHour * 60 + nowMinutes + 360
    }

    /**
      * 获取当前时间至今天凌晨的分钟数
      * @return
      */
    def getMinutesToTodayZero() :Int = {
        val nowMinutes= LocalDateTime.now().getMinute
        val nowHour = LocalDateTime.now().getHour
        nowHour * 60 + nowMinutes
    }

    def main(args: Array[String]): Unit = {
        print((DateTimeHH("1529543024889")))
        print(DateTimeHH("1529543024889").isInstanceOf[String])
    }
}
