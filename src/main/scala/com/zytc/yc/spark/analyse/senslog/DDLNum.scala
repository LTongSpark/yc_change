package com.zytc.yc.spark.analyse.senslog

import com.zytc.yc.spark.domain.SensLogInfo
import com.zytc.yc.spark.util.{JdbcUtil, ParseUserId}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
/**
  * DDL操作	            是否存在
  */
object DDLNum {
    def ddl(streamData: InputDStream[ConsumerRecord[String, String]],data:DStream[SensLogInfo]): Unit = {
        //对批次数据进行过滤分析操作
        data.filter(_.infotype.toLowerCase == "sensitivelog")
            .map(x => {
            var dataClass = 0
            var isViolations = 0 //是否违规(超出阀值)
            var flag = ""
            if (x.optional_dataClass == "") dataClass = 0
            else dataClass = x.optional_dataClass.toInt
            var affect = 0
            if (x.operation_act_affect == "") affect = 0
            else affect = x.operation_act_affect.toInt
            if (x.operation_act_do == "other") {
                flag = x.base_timestamp + "_" + x.interfaceNo
                isViolations = 1
            }
            Row(ParseUserId.parse(x.base_userId)._1, x.interfaceNo, x.infotype,
                x.base_userType, x.base_hostname, x.base_client, x.base_ip,
                x.base_timestamp, x.base_originalLogId, x.base_originalCMD,
                x.operation_type, x.operation_abnormal, x.operation_act_do,
                x.operation_act_useTime, affect, dataClass, x.optional_tablename,
                x.optional_databasename, x.optional_fieldname, x.origin_content,
                isViolations, flag, ParseUserId.parse(x.base_userId)._2)
        })
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    try {
                    val conn = JdbcUtil.createConn()
                    val ppst = conn.prepareStatement("insert into biz_log_isddl(" +
                        "user_id,interface_no,infotype,user_type,hostname,client,ip," +
                        "timestamp,original_logid,original_cmd,type,abnormal,opra_type," +
                        "use_time,affect,data_class,table_name,database_name,field_name," +
                        "origin_content,isViolations ,abnormal_tag,send_to) " +
                        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
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
                        ppst.setInt(16, e.getInt(15))
                        ppst.setString(17, e.getString(16))
                        ppst.setString(18, e.getString(17))
                        ppst.setString(19, e.getString(18))
                        ppst.setString(20, e.getString(19))
                        ppst.setInt(21, e.getInt(20))
                        ppst.setString(22, e.getString(21))
                        ppst.setString(23, e.getString(22))
                        println("biz_log_isddl" + "插入完成")
                        ppst.executeUpdate()
                    }
                    conn.close()
                    ppst.close()
                    }catch {
                        case e:Exception =>print("")
                    }
                })
            })
    }

}
