package com.zytc.yc.spark.analyse.senslog

import com.zytc.yc.spark.config.GlobalConfig
import com.zytc.yc.spark.domain.SensLogInfo
import com.zytc.yc.spark.util.{JdbcUtil, ParseUserId}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * 查询/导出数据量异常
  * (JAVA)时间约束:前一天的18:00 到当天的18:00,步长1分钟
  * (Scala) window:[长度不需计算,推进频率1分钟]
  */
object AffectAbnormal {

    /**
      * 分析数据
      * @param streamData 单一批次数据
      */
    def affect(streamData: InputDStream[ConsumerRecord[String, String]] ,data:DStream[SensLogInfo]): Unit = {
        //获取系统阈值
        val thresholdValue: Int = GlobalConfig.threshold.getOrElse("affectAbnormal", 100000)
        print(thresholdValue)
        //对批次数据进行过滤分析操作
       data.filter(_.infotype.toLowerCase == "sensitivelog")
            .filter(x => (x.operation_act_do == "select")) //做非可用数据的清洗
            .map(x => {
            var flag = ""
            var dataClass = 0
            var isViolations = 0 //是否违规(超出阀值)
            if (x.optional_dataClass == "") dataClass = 0
            else dataClass = x.optional_dataClass.toInt
            var affect = 0
            if (x.operation_act_affect == "") affect = 0
            else affect = x.operation_act_affect.toInt
            if (affect >= thresholdValue) {
                isViolations = 1
                flag = x.base_timestamp + "_" + x.interfaceNo
            }
            Row(ParseUserId.parse(x.base_userId)._1, x.interfaceNo, x.infotype,
                x.base_userType, x.base_hostname, x.base_client, x.base_ip,
                x.base_timestamp, x.base_originalLogId, x.base_originalCMD,
                x.operation_type, x.operation_abnormal,
                x.operation_act_do, x.operation_act_useTime, affect,
                dataClass, x.optional_tablename, x.optional_databasename,
                x.optional_fieldname, x.origin_content,
                isViolations, flag, ParseUserId.parse(x.base_userId)._2)
        }).foreachRDD(rdd => {
                JdbcUtil.insertTable(rdd ,"biz_log_affectabnormal")
            })
    }
}
