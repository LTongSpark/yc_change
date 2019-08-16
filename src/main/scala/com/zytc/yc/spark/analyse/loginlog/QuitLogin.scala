package com.zytc.yc.spark.analyse.loginlog

import com.zytc.yc.spark.common.GlobalConstant
import com.zytc.yc.spark.domain.LoadLogInfo
import com.zytc.yc.spark.util.{JdbcUtil, ParseUserId, Utility}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * 离职人员登录
  * (JAVA)时间约束:步长1分钟
  * (Scala)无约束
  */
object QuitLogin {
    def quitLogin( streamData: InputDStream[ConsumerRecord[String, String]],data:DStream[LoadLogInfo]): Unit = {
        //从数据库中取出离职的人
        val input_url = GlobalConstant.MYSQL_URL +
            "?characterEncoding=utf8&user=" + GlobalConstant.MYSQL_USERNAME +
            "&password=" + GlobalConstant.MYSQL_PASSWORD
        val prop = Utility.prop()
        val spark = SparkSession.builder
            .config(new SparkConf())
            .getOrCreate()
        val key = spark.read.jdbc(input_url, "sys_user", prop)
        key.createOrReplaceTempView("black_table")
        val userId = spark.sql("select username,hr_status from black_table")
        val key_object = userId.rdd.map(x => (x.getString(0), x.getString(1)))
        //日志解析
        val json = data.filter(_.base_userId.length > 0)
            .map(x => (ParseUserId.parse(x.base_userId)._1, (x.operation_act_result,
                x.interfaceNo, x.infotype, x.base_userType, x.base_hostname, x.base_client,
                x.base_ip, x.base_timestamp, x.base_originalLogId, x.base_originalCMD,
                x.operation_type, x.operation_abnormal, x.operation_act_do,
                x.optional_dataClass, x.optional_tablename, x.optional_databasename,
                x.optional_fieldname, x.origin_content , ParseUserId.parse(x.base_userId)._2)))
        json.transform(x => {
            x.rightOuterJoin(key_object, 4)
        })
            .filter(x => (x._2._1 != None))
            .map(x => {
                var dataClass = 0
                var flag = x._2._1.get._8 + "_" + x._2._1.get._2
                if (x._2._1.get._14 == "") dataClass = 0
                else dataClass = x._2._1.get._14.toInt

                var isViolations = 1
                if (x._2._2 == "A") {
                    isViolations = 0
                    flag = ""
                }
                //获取当前人的离职状态
                Row(x._1, x._2._1.get._1, x._2._1.get._2, x._2._1.get._3,
                    x._2._1.get._4, x._2._1.get._5, x._2._1.get._6,
                    x._2._1.get._7, x._2._1.get._8, x._2._1.get._9, x._2._1.get._10,
                    x._2._1.get._11, x._2._1.get._12, x._2._1.get._13,
                    dataClass, x._2._1.get._15, x._2._1.get._16,
                    x._2._1.get._17, x._2._1.get._18, isViolations, flag,
                    x._2._1.get._19)
            })
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    val conn = JdbcUtil.createConn()
                    val ppst = conn.prepareStatement("insert into biz_log_quitlogin(" +
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
