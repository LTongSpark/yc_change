package com.zytc.yc.spark.analyse.senslog

import com.zytc.yc.spark.Repository.ViolationsRepository
import com.zytc.yc.spark.config.GlobalConfig
import com.zytc.yc.spark.domain.SensLogInfo
import com.zytc.yc.spark.util.{JdbcUtil, ParseUserId}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  *
  * (JAVA)时间约束:当前时刻向前10分钟的插入次数，步长1分钟
  * (Scala) window:[长度10分钟,推进频率1分钟]
  */
object ShortTimeAbnomal {
    def shortTime(ssc: StreamingContext, streamData: InputDStream[ConsumerRecord[String, String]],data:DStream[SensLogInfo]): Unit = {
        //计算单批次查询次数
        var pcMap: Map[String, Int] = Map()
        //获取系统阈值
        val thresholdValue: Int = GlobalConfig.threshold.getOrElse("shortTimeAbnoraml", 10)
        //对批次数据进行过滤操作(选出select类型日志,并转置日志对象类型SensLogInfo->Tuple2(do,(TupleN)))
        val json = data.filter(_.infotype.toLowerCase == "sensitivelog")
            .filter(_.base_userId.length > 0)
            .filter(x => x.operation_act_do == "insert")
            .map(x => (x.operation_act_do, (x.base_userId, x.interfaceNo, x.infotype,
                x.base_userType, x.base_hostname, x.base_client, x.base_ip,
                x.base_timestamp, x.base_originalLogId, x.base_originalCMD,
                x.operation_type, x.operation_abnormal, x.operation_act_useTime,
                x.operation_act_affect, x.optional_dataClass, x.optional_tablename,
                x.optional_databasename, x.optional_fieldname, x.origin_content)))
            .window(Minutes(10), Minutes(1))
        val insert = List("insert")
        val key_object = ssc.sparkContext.parallelize(insert)
            .map(x => (x, true))
        json.transform(x => {
            x.leftOuterJoin(key_object, 4)
        })
            .filter(x => (x._2._2.getOrElse(false) == true))
            .map(x => {
                var dataClass = 0
                var affect = 0
                var isViolations = 0 //是否违规(超出阀值)
                var user = ParseUserId.parse(x._2._1._1)._1
                if (x._2._1._15.equals("")) dataClass = 0
                else dataClass = x._2._1._15.toInt
                if (x._2._1._14.equals("")) affect = 0
                else affect = x._2._1._14.toInt
                //计算操作次数
                var currentNum: Int = pcMap.getOrElse(user, 0) + 1
                pcMap = pcMap.updated(user, currentNum)
                //判断是否违规
                if (currentNum > thresholdValue) isViolations = 1
                (x._1, (user, x._2._1._2, x._2._1._3,
                    x._2._1._4, x._2._1._5, x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9,
                    x._2._1._10, x._2._1._11, x._2._1._12, x._2._1._13, affect, dataClass,
                    x._2._1._16, x._2._1._17, x._2._1._18, x._2._1._19, currentNum, isViolations,
                    ParseUserId.parse(x._2._1._1)._2))
            }) // x => (userId,do,timestamp...)
            //处理下一步的并发
            .transform(x => {
            x.leftOuterJoin(key_object, 4)
        })
            .filter(x => (x._2._2.getOrElse(false) == true))
            .map(x => Row(x._2._1._1, x._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5,
                x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9, x._2._1._10, x._2._1._11,
                x._2._1._12, x._2._1._13, x._2._1._14, x._2._1._15, x._2._1._16, x._2._1._17,
                x._2._1._18, x._2._1._19, x._2._1._20, x._2._1._21, x._2._1._22))
            //(二)与历史做比较，进行数据过滤、更新历史记录
            //原则:要不要(次数小于等于历史记录的,时间等于历史记录的不要，其他的要)
            //    不要的处理:(1)次数小于历史的,无操作，不返回值
            //             (2)次数与时间等于历史的,将历史的次数更新为0[也就是直接删掉对应历史]
            //    要的处理: 更新次数为当前次数,时间更新为本条日志的timestamp
            .map(x => {
            //获取历史记录
            val userTuple3 = ViolationsRepository.Repository.get("ShortTimrAbnormal")
                .get.getOrElse(x.getString(0), Tuple3(0, "", x.getString(8) + "_" + x.getString(2))) //(累计次数,最后一次操作时间)
            if (userTuple3._1 >= thresholdValue & x.getString(8) == userTuple3._2) {
                var historyMap = ViolationsRepository.Repository.get("ShortTimrAbnormal").
                    get.updated(x.getString(0), (0, x.getString(8), x.getString(8) + "_" + x.getString(2)))
                ViolationsRepository.Repository = ViolationsRepository.Repository.updated("ShortTimrAbnormal", historyMap)
                Row(Nil)
            } else {
                if (x.getInt(20) <= userTuple3._1 || x.getString(8) <= userTuple3._2) { //不要的
                    if (x.getString(8) == userTuple3._2) {
                        //删掉历史
                        var historyMap = ViolationsRepository.Repository.get("ShortTimrAbnormal").
                            get.updated(x.getString(0), (0, x.getString(8), userTuple3._3))
                        ViolationsRepository.Repository = ViolationsRepository.Repository.updated("ShortTimrAbnormal", historyMap)
                    }
                    Row(Nil)
                } else {
                    //更新次数为当前次数,时间更新为本条日志的timestamp
                    var historyMap = ViolationsRepository.Repository.get("ShortTimrAbnormal").
                        get.updated(x.getString(0), (x.getInt(20), x.getString(8), userTuple3._3))
                    ViolationsRepository.Repository = ViolationsRepository.Repository.updated("ShortTimrAbnormal", historyMap)
                    Row(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
                        x.getString(4), x.getString(5), x.getString(6), x.getString(7),
                        x.getString(8), x.getString(9), x.getString(10), x.getString(11),
                        x.getString(12), x.getString(13), x.getInt(14), x.getInt(15),
                        x.getString(16), x.getString(17), x.getString(18), x.getString(19),
                        x.getInt(20), x.getInt(21), userTuple3._3, x.getString(22))
                }
            }
        })
            .filter(!_.equals(Row(Nil))).foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                try {
                    val conn = JdbcUtil.createConn()
                    val ppst = conn.prepareStatement("insert into biz_log_shorttimeabnomal(" +
                        "user_id,opra_type,interface_no,infotype,user_type,hostname,client,ip," +
                        "timestamp,original_logid,original_cmd,type,abnormal,use_time,affect," +
                        "data_class,table_name,database_name,field_name,origin_content," +
                        "influen_num,isViolations,abnormal_tag ,send_to) " +
                        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
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
                        ppst.setInt(22, e.getInt(21))
                        ppst.setString(23, e.getString(22))
                        ppst.setString(24, e.getString(23))
                        println("biz_log_shorttimeabnomal" + "插入完成")
                        ppst.executeUpdate()
                    }
                    conn.close()
                    ppst.close()
                } catch {
                    case e: Exception => print("")
                }
            })

        })
    }
}
