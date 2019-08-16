package com.zytc.yc.spark.analyse.loginlog

import com.zytc.yc.spark.Repository.ViolationsRepository
import com.zytc.yc.spark.config.GlobalConfig
import com.zytc.yc.spark.domain.LoadLogInfo
import com.zytc.yc.spark.util.{JdbcUtil, ParseUserId}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * 连续登录失败
  * (JAVA)时间约束:截止当前时刻为止,一分钟一个时间单位,步长1分钟
  * (Scala) window:[长度5分钟,推进频率1分钟]
  */
object FreqFail {
    def freqFail(ssc: StreamingContext, streamData: InputDStream[ConsumerRecord[String, String]],data:DStream[LoadLogInfo]): Unit = {
        //计算单批次查询次数
        var pcMap: Map[String, Int] = Map()
        //获取系统阈值
        val thresholdValue: Int = GlobalConfig.threshold.getOrElse("FreqFail", 10)
        val json = data.filter(_.base_userId.length > 0)
            .map(x => {
                ("login", (x.operation_act_result, x.base_userId, x.interfaceNo,
                    x.infotype, x.base_userType, x.base_hostname, x.base_client,
                    x.base_ip, x.base_timestamp, x.base_originalLogId, x.base_originalCMD,
                    x.operation_type, x.operation_abnormal, x.operation_act_do,
                    x.optional_dataClass, x.optional_tablename, x.optional_databasename,
                    x.optional_fieldname, x.origin_content))
            })
            .window(Minutes(5), Minutes(1))
        val fail = List("login")
        val key_object = ssc.sparkContext.parallelize(fail)
            .map(x => (x, true))

        json.transform(x => {
            x.leftOuterJoin(key_object)
        })
            .filter(x => (x._2._2.getOrElse(false) == true))
            .map(x => {
                var dataClass = 0
                var isViolations = 0 //是否违规(超出阀值)
                var currentNum: Int = 0
                var user = ParseUserId.parse(x._2._1._2)._1
                if (x._2._1._1.equals("fail")) {
                    currentNum = pcMap.getOrElse(user, 0) + 1
                    pcMap = pcMap.updated(user, currentNum)
                } else {
                    currentNum = 0
                    pcMap = pcMap.updated(user, currentNum)
                }
                if (x._2._1._15.equals("")) dataClass = 0
                else dataClass = x._2._1._15.toInt
                //判断是否违规
                if (currentNum > thresholdValue) isViolations = 1
                (x._1, (x._2._1._1, user, x._2._1._3, x._2._1._4, x._2._1._5,
                 x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9, x._2._1._10,
                 x._2._1._11, x._2._1._12, x._2._1._13, x._2._1._14, dataClass,
                 x._2._1._16, x._2._1._17, x._2._1._18, x._2._1._19, currentNum,
                 isViolations, ParseUserId.parse(x._2._1._2)._2))
            })
            .transform(x => {
            x.leftOuterJoin(key_object, 4)
        })
            .filter(x => (x._2._2.getOrElse(false) == true))
            .map(x => (x._2._1._2, x._2._1._1, x._2._1._3, x._2._1._4, x._2._1._5,
                x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9, x._2._1._10,
                x._2._1._11, x._2._1._12, x._2._1._13, x._2._1._14, x._2._1._15,
                x._2._1._16, x._2._1._17, x._2._1._18, x._2._1._19, x._2._1._20,
                x._2._1._21, x._2._1._22))
            .map(x => {
                //获取历史记录
                val userTuple3 = ViolationsRepository.Repository.get("FreqFail").
                    get.getOrElse(x._1, Tuple3(0, "", x._9 + "_" + x._3)) //(累计次数,最后一次操作时间)
                if (userTuple3._1 == 0 & x._9 == userTuple3._2) {
                    //更新标记
                    var historyMap = ViolationsRepository.Repository.get("FreqFail").
                        get.updated(x._1, (0, x._9, x._9 + "_" + x._3))
                    ViolationsRepository.Repository = ViolationsRepository.Repository.updated("FreqFail", historyMap)
                    Row(Nil)
                } else {
                    if (x._20 == 0 & x._9 > userTuple3._2) {
                        var historyMap = ViolationsRepository.Repository.get("FreqFail").
                            get.updated(x._1, (userTuple3._1, x._9, userTuple3._3))
                        ViolationsRepository.Repository = ViolationsRepository.Repository.updated("FreqFail", historyMap)
                        Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11,
                            x._12, x._13, x._14, x._15, x._16, x._17, x._18, x._19, x._20, x._21, "",x._22)
                    } else {
                        if (x._20 <= userTuple3._1 || x._9 <= userTuple3._2) { //不要的
                            if (x._9 == userTuple3._2) {
                                //删掉历史
                                var historyMap = ViolationsRepository.Repository.get("FreqFail").
                                    get.updated(x._1, (0, x._9, userTuple3._3))
                                ViolationsRepository.Repository = ViolationsRepository.Repository.updated("FreqFail", historyMap)
                            }
                            Row(Nil)
                        } else {
                            //更新次数为当前次数,时间更新为本条日志的timestamp
                            var historyMap = ViolationsRepository.Repository.get("FreqFail").
                                get.updated(x._1, (x._20, x._9, userTuple3._3))
                            ViolationsRepository.Repository = ViolationsRepository.Repository.updated("FreqFail", historyMap)
                            println("更改后的时间" + ViolationsRepository.Repository.get("FreqFail"))
                            Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7,
                                x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15, x._16, x._17,
                                x._18, x._19, x._20, x._21, userTuple3._3, x._22)
                        }
                    }
                }
            }).filter(!_.equals(Row(Nil)))
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    val conn = JdbcUtil.createConn()
                    val ppst = conn.prepareStatement("insert into biz_log_freqfail(" +
                        "user_id,result,interface_no,infotype,user_type,hostname," +
                        "client,ip,timestamp,original_logid,original_cmd,type,abnormal," +
                        "opra_type,data_class,table_name,database_name,field_name,origin_content," +
                        "login_fail_count,isViolations,abnormal_tag,send_to" +
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
                        ppst.setInt(15, e.getInt(14))
                        ppst.setString(16, e.getString(15))
                        ppst.setString(17, e.getString(16))
                        ppst.setString(18, e.getString(17))
                        ppst.setString(19, e.getString(18))
                        ppst.setInt(20, e.getInt(19))
                        ppst.setInt(21, e.getInt(20))
                        ppst.setString(22, e.getString(21))
                        ppst.setString(23, e.getString(22))
                        println("biz_log_freqfail" + "插入完成")
                        ppst.executeUpdate()
                    }
                    conn.close()
                    ppst.close()
                })
            })
    }
}
