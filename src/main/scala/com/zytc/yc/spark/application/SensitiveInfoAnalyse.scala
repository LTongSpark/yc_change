package com.zytc.yc.spark.application

import com.zytc.yc.spark.analyse.senslog._
import com.zytc.yc.spark.common.GlobalConstant
import com.zytc.yc.spark.domain.SensLogInfo
import com.zytc.yc.spark.parsejson.SensBean
import com.zytc.yc.spark.util.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * 操作类型	删除操作	    是否存在			                    安全小组，应用管理员
  * DDL操作	            是否存在			                    安全小组，应用管理员
  * 次数	查询/导出次数异常	动态阈值	    天		                安全小组，应用管理员
  * 短时间内插入次数异常	动态阈值			                    安全小组，应用管理员
  * 数量	查询/导出数据量异常	动态阈值	    天、周、月、单次	        安全小组，应用管理员
  * 关键对象更新操作	    是否存在			                    安全小组，应用管理员
  *
  */
object SensitiveInfoAnalyse {
    def main(args: Array[String]): Unit = {
        //设置日志级别
        Logger.getLogger("org").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName("SensitiveInfoAnalyse")
        val ssc = new StreamingContext(conf, Minutes(GlobalConstant.KAFKA_BATCH_PERIOD))
        val streamData: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.Kafka( ssc, 2)
        //取出所有的数据
         val jsonDstream:DStream[SensLogInfo] = streamData.map(_.value())
                                .map(rdd => SensBean.operationBean(rdd))
        CountSelectAbnormal.countSelect(ssc,streamData,jsonDstream)
        //短时间
        ShortTimeAbnomal.shortTime(ssc,  streamData,jsonDstream)
        //查询导出数据异常
        AffectAbnormal.affect( streamData,jsonDstream)
        //删除操作是否存在
        DeleteNum.delete(streamData,jsonDstream)
        //ddl操作是否存在
        DDLNum.ddl(streamData,jsonDstream)
        //ddl操作是否存在
        ApiLog.api(streamData,jsonDstream)
        ssc.start()
        ssc.awaitTermination()
    }
}
