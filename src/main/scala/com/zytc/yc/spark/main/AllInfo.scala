package com.zytc.yc.spark.main

import com.zytc.yc.spark.analyse.authlog.{NotWorkOper, SamePer}
import com.zytc.yc.spark.analyse.loginlog._
import com.zytc.yc.spark.analyse.senslog._
import com.zytc.yc.spark.common.GlobalConstant
import com.zytc.yc.spark.login.analyse._
import com.zytc.yc.spark.seneitiveInfo.analyse._
import com.zytc.yc.spark.util.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

/**
  * Author:   LTong
  * Date:     2019-04-29 09:08
  */
object AllInfo {
    def main(args: Array[String]): Unit = {
        //设置日志级别
        Logger.getLogger("org").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName("SensitiveInfoAnalyse")
        val ssc = new StreamingContext(conf, Minutes(GlobalConstant.KAFKA_BATCH_PERIOD))
        val streamData: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.Kafka(ssc, 1)


        /**
          * 操作日志的输出
          */

        //次数查询/导出次数异常
//        CountSelectAbnormal.countSelect(ssc, streamData)
//        //短时间
//        ShortTimeAbnomal.shortTime(ssc, streamData)
//        //查询导出数据异常
//        AffectAbnormal.affect(streamData)
//        //删除操作是否存在
//        DeleteNum.delete(streamData)
//        //ddl操作是否存在
//        DDLNum.ddl(streamData)
//        //ddl操作是否存在
//        ApiLog.api(streamData)
//
//        /**
//          * 登录日志的输出
//          */
//
//        //抽取出需要的数据并判断是否为业务部门的数据
//        NotWorkLogin.notWorkLogin(ssc, streamData)
//        //离职后登陆
//        QuitLogin.quitLogin(streamData)
//        //一天内登录的失败的次数
//        DayLoginFail.loginFair(ssc, streamData)
//        //一个小时内登录的失败次数
//        HourLoginFail.loginFair(ssc, streamData)
//        //10分钟登录的次数
//        MinLogFail.minLogFail(ssc, streamData)
//        //连续失败的次数
//        FreqFail.freqFail(ssc, streamData)
//
//        /**
//          * 权限日志的输出
//          */
//
//        //非工作时间授权
//        NotWorkOper.notWorkOper(ssc, streamData)
//        //短时间内相同权限授予并收回
//        SamePer.samePer(streamData)
        //关键对象授权异常
        //KeyObject.key(config, streamData, mysqlStatisticsUrl)
        //非域账号
        //NotAreaAccount.notArea(config, streamData, mysqlStatisticsUrl)
        //DB/OS变化
        //DSChange.DS(config, streamData, mysqlStatisticsUrl)
        ssc.start()
        ssc.awaitTermination()
    }

}
