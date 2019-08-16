package com.zytc.yc.spark.analyse.loginlog

import com.zytc.yc.spark.domain.LoadLogInfo
import com.zytc.yc.spark.util.{DateUtil, JdbcUtil, ParseUserId}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * 非工作时间登录
  * (JAVA)时间约束:前一天的18:00 到当天的18:00,步长1分钟
  * (Scala) window:[长度需计算,现在至昨天的18:00,推进频率1分钟]
  */
object NotWorkLogin {
    def notWorkLogin(ssc: StreamingContext, streamData: InputDStream[ConsumerRecord[String, String]],data:DStream[LoadLogInfo]): Unit = {
        data.filter(_.base_userId.length > 0)
            .map(x => {
                val workTime = List("09", "10", "11", "12", "13", "14", "15", "16", "17")
                //违规标记
                var flag = x.base_timestamp + x.interfaceNo
                var dataClass = 0
                //是否违规
                var isViolations = 1
                if (x.optional_dataClass.equals("")) dataClass = 0
                else dataClass = x.optional_dataClass.toInt
                //工作时间
                for (i <- workTime) {
                    if (DateUtil.DateTimeHH(x.base_timestamp).substring(8, 10) == i) {
                        isViolations = 0
                        flag = ""
                    }
                }
                Row(ParseUserId.parse(x.base_userId)._1, x.operation_act_result,
                    x.interfaceNo, x.infotype, x.base_userType, x.base_hostname,
                    x.base_client, x.base_ip, x.base_timestamp, x.base_originalLogId,
                    x.base_originalCMD, x.operation_type, x.operation_abnormal,
                    x.operation_act_do, dataClass, x.optional_tablename,
                    x.optional_databasename, x.optional_fieldname, x.origin_content,
                    isViolations, flag, ParseUserId.parse(x.base_userId)._2)
            })
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    val conn = JdbcUtil.createConn()
                    val ppst = conn.prepareStatement("insert into biz_log_notworklogin(" +
                        "user_id,result,interface_no,infotype,user_type,hostname,client,ip," +
                        "timestamp,original_logid,original_cmd,type,abnormal,opra_type,data_class," +
                        "table_name,database_name,field_name,origin_content,isViolations," +
                        "abnormal_tag,send_to) " +
                        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                    for (e <- it) {
                        ppst.setString(1, e.getString(0))
                        ppst.setString(2, e.getString(1))
                        ppst.setString(3, e.getString(2))
                        ppst.setString(4, e.getString(3))
                        ppst.setString(5, e.getString(4))
                        ppst.setString(6, e.getString(5))
                        ppst.setString(7, e.getString(6))
                        ppst.setString(8, e.getString(7))
                        ppst.setString(9, e.getString(8))
                        ppst.setString(10, e.getString(9))
                        ppst.setString(11, e.getString(10))
                        ppst.setString(12, e.getString(11))
                        ppst.setString(13, e.getString(12))
                        ppst.setString(14, e.getString(13))
                        ppst.setInt(15, e.getInt(14))
                        ppst.setString(16, e.getString(15))
                        ppst.setString(17, e.getString(16))
                        ppst.setString(18, e.getString(17))
                        ppst.setString(19, e.getString(18))
                        ppst.setInt(20, e.getInt(19))
                        ppst.setString(21, e.getString(20))
                        ppst.setString(22, e.getString(21))
                        println("biz_log_NotworkLogin" + "插入完成数据")
                        ppst.executeUpdate()
                    }
                    conn.close()
                    ppst.close()
                })
            })
    }
}
