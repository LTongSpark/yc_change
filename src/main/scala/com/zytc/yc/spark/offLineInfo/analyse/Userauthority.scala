package com.zytc.yc.spark.offLineInfo.analyse

import java.util

import com.typesafe.config.Config
import com.zytc.yc.spark.parsejson.AuthBean
import com.zytc.yc.spark.util.{DateUtil, HbaseUtil, JdbcUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

object Userauthority {
    def author(sc: SparkContext,regex: String): Unit ={
        import scala.collection.JavaConverters._
        val value: util.List[String] = HbaseUtil.rangeFilterScan(regex)
        val buffer: scala.collection.mutable.Buffer[String] = value.asScala
        val json = sc.parallelize(buffer)
        json.map(rdd => AuthBean.authLogBean(rdd))
            .filter(_.infotype.toLowerCase == "authlog")
            .map(x =>{
        var dataClass = 0
        if ( x.optional_dataClass== "") dataClass = 0
        else dataClass = x.optional_dataClass.toInt
                Row(x.infotype ,x.interfaceNo ,x.base_userId ,x.base_userType,
                    x.base_hostname ,x.base_client ,x.base_ip ,DateUtil.parseDateTime(x.base_timestamp),
                    x.base_originalLogId ,x.base_originalCMD ,x.operation_type ,
                    x.operation_act_do,x.operation_act_targetUserId ,x.operation_act_original,
                    x.operation_act_now,x.operation_abnormal ,dataClass,
                    x.optional_databasename ,x.optional_tablename ,x.optional_fieldname
            )})
            .foreachPartition(it => {
                val conn = JdbcUtil.createConn()
                val ppst = conn.prepareStatement("insert into userauthority_info(" +
                    "Infotype,Interface,userId,userType,hostname,client,ip,timestamp," +
                    "originalLogId,originalCMD,type,Do,targetUserId,original," +
                    "now,abnormal,Classes,Databasename,Tablename,Fieldname" +
                    ") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
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
                    ppst.setInt(17, e.getInt(16))
                    ppst.setString(18, e.getString(17))
                    ppst.setString(19, e.getString(18))
                    ppst.setString(20, e.getString(19))
                    println("userauthority_info" + "插入完成")
                    ppst.executeUpdate()
                }
                conn.close()
                ppst.close()
            })



    }


}
