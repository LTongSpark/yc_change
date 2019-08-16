package com.zytc.yc.spark.analyse.authlog

import com.zytc.yc.spark.domain.AuthLogInfo
import com.zytc.yc.spark.util.{DateUtil, JdbcUtil, ParseUserId}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * 非工作时间授权
  * (JAVA)时间约束:前一天的18:00 到当天的18:00,步长1分钟
  * (Scala) window:[长度需计算,现在至昨天的18:00,推进频率1分钟]
  */
object NotWorkOper {
    def notWorkOper(ssc: StreamingContext, streamData: InputDStream[ConsumerRecord[String, String]],data:DStream[AuthLogInfo]): Unit = {
            data.filter(_.base_userId.length > 0)
            .map(x => {
                val workTime = List("09", "10", "11", "12", "13", "14", "15", "16", "17")
                var flag = x.base_timestamp + "_" + x.interfaceNo
                //是否违规
                var isViolations = 1
                var dataClass = 0
                if (x.optional_dataClass.equals("")) dataClass = 0
                else dataClass = x.optional_dataClass.toInt
                //工作时间
                for (i <- workTime) {
                    if (DateUtil.DateTimeHH(x.base_timestamp).substring(8, 10) == i) {
                        isViolations = 0
                        flag = ""
                    }
                }
                Row(ParseUserId.parse(x.base_userId)._1, x.interfaceNo, x.infotype, x.base_userType,
                    x.base_hostname, x.base_client, x.base_ip, x.base_timestamp, x.base_originalLogId,
                    x.base_originalCMD, x.operation_type, x.operation_abnormal, x.operation_act_do,
                    x.operation_act_targetRoleIds, x.operation_act_targetUserId, x.operation_act_original,
                    x.operation_act_now, dataClass, x.optional_tablename, x.optional_databasename,
                    x.optional_fieldname, x.origin_content, isViolations, flag, ParseUserId.parse(x.base_userId)._2)
            })
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    val conn = JdbcUtil.createConn()
                    val ppst = conn.prepareStatement("insert into biz_log_notworkoper(" +
                        "user_id,interface_no,infotype,user_type,hostname,client,ip," +
                        "timestamp,original_logid,original_cmd,type,abnormal,opra_type," +
                        "target_roleids,target_userid,original,now,data_class,table_name," +
                        "database_name,field_name,origin_content,isViolations," +
                        "abnormal_tag,send_to) " +
                        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
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
                        ppst.setString(15, e.getString(14))
                        ppst.setString(16, e.getString(15))
                        ppst.setString(17, e.getString(16))
                        ppst.setInt(18, e.getInt(17))
                        ppst.setString(19, e.getString(18))
                        ppst.setString(20, e.getString(19))
                        ppst.setString(21, e.getString(20))
                        ppst.setString(22, e.getString(21))
                        ppst.setInt(23, e.getInt(22))
                        ppst.setString(24, e.getString(23))
                        ppst.setString(25, e.getString(24))
                        println("biz_log_NotworkOper" + "插入完成")
                        ppst.executeUpdate()
                    }
                    conn.close()
                    ppst.close()
                })
            })
    }
}
