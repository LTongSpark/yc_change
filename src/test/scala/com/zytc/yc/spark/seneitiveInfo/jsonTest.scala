package com.zytc.yc.spark.seneitiveInfo

import java.util

import com.zytc.yc.spark.parsejson.SensBean
import com.zytc.yc.spark.util.{HbaseUtil, JdbcUtil, Utility}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}


object jsonTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("app")
        val sc = new SparkContext(conf)

        import scala.collection.JavaConverters._
        val value: util.List[String] = HbaseUtil.rangeFilterScan("20181127")
        val buffer: scala.collection.mutable.Buffer[String] = value.asScala
        val jsom1 = sc.parallelize(buffer)
        jsom1.map(rdd => SensBean.operationBean(rdd))
            .filter(_.infotype.toLowerCase == "sensitivelog")
            .map(x => {
                var affect = 0
                if (x.operation_act_affect == "") affect = 0
                else affect = x.operation_act_affect.toInt
                Row(x.infotype, x.interfaceNo, x.base_originalLogId,
                    x.base_userId, x.base_userType, x.operation_act_do,
                    x.base_timestamp, affect, x.base_hostname,
                    x.base_client)
            })
            .foreachPartition(it => {
                val conn = JdbcUtil.createConn()
                val ppst = conn.prepareStatement("insert into yb_data_log(" +
                    "platform,original_log_id,account,account_type,cmd,start_time,output,device,client" +
                    ") values(?,?,?,?,?,?,?,?,?)")
                for (e <- it) {
                    ppst.setString(1, e.getString(1))
                    ppst.setString(2, e.getString(2))
                    ppst.setString(3, e.getString(3))
                    ppst.setString(4, e.getString(4))
                    ppst.setString(5, e.getString(5))
                    ppst.setString(6, e.getString(6))
                    ppst.setInt(7, e.getInt(7))
                    ppst.setString(8, e.getString(8))
                    ppst.setString(9, e.getString(9))
                    println("yb_data_log" + "插入完成")
                    ppst.executeUpdate()
                }
                conn.close()
                ppst.close()
            })
    }

}
