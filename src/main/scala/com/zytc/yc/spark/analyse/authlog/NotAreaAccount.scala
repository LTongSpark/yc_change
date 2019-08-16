package com.zytc.yc.spark.analyse.authlog

import com.typesafe.config.Config
import com.zytc.yc.spark.parsejson.AuthBean
import com.zytc.yc.spark.util.{JdbcUtil, Utility}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream

/**
  * @author LTong
  * @date 2019-06-21 下午 5:41
  */
//非域账号
object NotAreaAccount {
    def notArea(config: Config, streamData: InputDStream[ConsumerRecord[String, String]], mysqlStatisticsUrl: String): Unit = {
        val input_url = config.getString("mysql.url") +
            "?characterEncoding=utf8&user=" + config.getString("mysql.username") +
            "&password=" + config.getString("mysql.password")
        val prop = Utility.prop()
        val spark = SparkSession.builder
            .config(new SparkConf())
            .getOrCreate()
        val key = spark.read.jdbc(input_url, "sys_user", prop)
        key.createOrReplaceTempView("white_table")
        val userId = spark.sql("select username from white_table")
        val key_object = userId.rdd.map(x => (x.getString(0), true))

        //日志解析
        val json = streamData.map(_.value())
            .map(rdd => AuthBean.authLogBean(rdd))
            .filter(_.base_userId.length > 0)
            .map(x => (x.base_userId, (
                x.interfaceNo,
                x.infotype,
                x.base_userType, x.base_hostname, x.base_client,
                x.base_ip, x.base_timestamp, x.base_originalLogId, x.base_originalCMD,
                x.operation_type, x.operation_abnormal,
                x.operation_act_do, x.operation_act_targetRoleIds, x.operation_act_targetUserId,
                x.operation_act_original, x.operation_act_now,
                x.optional_dataClass, x.optional_tablename, x.optional_databasename, x.optional_fieldname, x.origin_content
            )))
        json.transform(x => {
            x.leftOuterJoin(key_object, 4)
        })
            .map(x => {
                var dataClass = 0
                if (x._2._1._17 == "") dataClass = 0
                else dataClass = x._2._1._17.toInt
                var isViolations = 0
                if (x._2._2 == None) isViolations = 1
                //获取当前人的离职状态
                Row(x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5,
                    x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9, x._2._1._10,
                    x._2._1._11, x._2._1._12, x._2._1._13, x._2._1._14, x._2._1._15, x._2._1._16,
                    dataClass, x._2._1._18, x._2._1._19, x._2._1._20, x._2._1._21, isViolations)
            })
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    val conn = JdbcUtil.createConn(config)
                    val ppst = conn.prepareStatement("insert into biz_log_iskeyobject(" +
                        "user_id,interface_no,infotype,user_type,hostname,client,ip,timestamp," +
                        "original_logid,original_cmd,type,abnormal,opra_type,target_roleids,target_userid," +
                        "original,now,data_class,table_name,database_name,field_name,origin_content,isViolations" +
                        ") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
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
                        println("biz_log_NotworkOper" + "插入完成")
                        ppst.executeUpdate()
                    }
                    conn.close()
                    ppst.close()
                })
            })
    }
}
