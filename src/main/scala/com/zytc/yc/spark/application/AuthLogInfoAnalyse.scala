package com.zytc.yc.spark.application

import com.zytc.yc.spark.analyse.authlog.NotWorkOper
import com.zytc.yc.spark.common.GlobalConstant
import com.zytc.yc.spark.parsejson.AuthBean
import com.zytc.yc.spark.util.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * 1,非工作时间操作                          定值
  * 2，关键对象授权异常                        白名单
  * 3，短时间内相同权限授予并收回               是否存在  一天存在
  * 4，非域账号                              白名单
  * 5， DB/OS用户变化                        白名单
  */
object AuthLogInfoAnalyse {
    def main(args: Array[String]): Unit = {
        //设置日志级别
        Logger.getLogger("org").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName("authLogInfoAnalyse")
        val ssc = new StreamingContext(conf, Minutes(GlobalConstant.KAFKA_BATCH_PERIOD))
        val streamData: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.Kafka(ssc, 3)
        val jsonDstream = streamData.map(_.value())
          .map(rdd => AuthBean.authLogBean(rdd))

        //非工作时间授权
        NotWorkOper.notWorkOper(ssc, streamData,jsonDstream)
        //短时间内相同权限授予并收回
        //SamePer.samePer(streamData, config)
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
