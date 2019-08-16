package com.zytc.yc.spark.analyse.senslog

import com.zytc.yc.spark.domain.SensLogInfo
import com.zytc.yc.spark.util.{JdbcUtil, ParseUserId}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * api日志
  * window:[长度不需计算,推进频率1分钟]
  */

object ApiLog {
  def api(streamData: InputDStream[ConsumerRecord[String, String]], data: DStream[SensLogInfo]): Unit = {
    //对批次数据进行过滤分析操作
    data.filter(_.infotype.toLowerCase == "apilog")
      .map(x => {
        var dataClass = 0
        var isViolations = 0 //是否违规(超出阀值)
        var flag = x.base_timestamp + "_" + x.interfaceNo
        if (x.optional_dataClass == "") dataClass = 0
        else dataClass = x.optional_dataClass.toInt
        var affect = 0
        if (x.operation_act_affect == "") affect = 0
        else affect = x.operation_act_affect.toInt
        Row(ParseUserId.parse(x.base_userId)._1, x.interfaceNo, x.infotype,
          x.base_userType, x.base_hostname, x.base_client, x.base_ip,
          x.base_timestamp, x.base_originalLogId, x.base_originalCMD,
          x.operation_type, x.operation_abnormal, x.operation_act_do,
          x.operation_act_useTime, affect, dataClass, x.optional_tablename,
          x.optional_databasename, x.optional_fieldname, x.origin_content,
          isViolations, flag, ParseUserId.parse(x.base_userId)._2)
      }).foreachRDD(rdd => {
        JdbcUtil.insertTable(rdd, "biz_log_apilog")
      })
  }
}
