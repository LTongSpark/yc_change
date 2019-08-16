package com.zytc.yc.spark.offLineInfo

import com.zytc.yc.spark.offLineInfo.analyse.{SensitiveLog, Userauthority}
import com.zytc.yc.spark.util.{DateUtil, Utility}
import org.apache.spark.{SparkConf, SparkContext}

object OffLineAnalyse {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                //.setMaster("local[*]")
            .setAppName("OffLineAnalyse")
        val sc = new SparkContext(conf)
        //val config = Utility.parseConfFile("D:\\mobsf\\yc\\src\\main\\resources\\log_sta.conf")

        //获取前一个月的日期
        val endTime = DateUtil.parseLocalTime()
        //静态数据的获取
        SensitiveLog.sens( sc,endTime)
        Userauthority.author(sc,endTime)
    }
}
