package com.zytc.yc.spark.application

import com.zytc.yc.spark.analyse.loginlog._
import com.zytc.yc.spark.common.GlobalConstant
import com.zytc.yc.spark.domain.LoadLogInfo
import com.zytc.yc.spark.parsejson.LoadBean
import com.zytc.yc.spark.util.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * 通过加载json文件分析连续失败次数，一天内登录失败次数多，一小时内登录失败次数多，10分钟内登录次数多
  */

object LoginInfoAnalyse {
    def main(args: Array[String]): Unit = {
        //设置日志级别
        Logger.getLogger("org").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName("loginInfoAnalyse")
        val ssc = new StreamingContext(conf, Minutes(GlobalConstant.KAFKA_BATCH_PERIOD))
        val streamData: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.Kafka(ssc, 1)

        val jsonDstream:DStream[LoadLogInfo] = streamData.map(_.value())
          .map(rdd => LoadBean.loadLogBean(rdd))

        //抽取出需要的数据并判断是否为业务部门的数据
        NotWorkLogin.notWorkLogin(ssc, streamData,jsonDstream)
        //离职后登陆
        QuitLogin.quitLogin(streamData,jsonDstream)
        //一天内登录的失败的次数
        DayLoginFail.loginFair(ssc, streamData,jsonDstream)
        //一个小时内登录的失败次数
        HourLoginFail.loginFair(ssc, streamData,jsonDstream)
        //10分钟登录的次数
        MinLogFail.minLogFail(ssc, streamData,jsonDstream)
        //连续失败的次数
        FreqFail.freqFail(ssc, streamData,jsonDstream)
        //启动流
        ssc.start()
        //等待接待
        ssc.awaitTermination()
    }
}
