package com.zytc.yc.spark.util

/**
  * 数据库驱动程序
  */

import java.sql.DriverManager
import com.zytc.yc.spark.common.GlobalConstant
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object JdbcUtil {
  def createConn() = {
    val url = GlobalConstant.MYSQL_URL
    val username = GlobalConstant.MYSQL_USERNAME
    val password = GlobalConstant.MYSQL_PASSWORD
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection(url, username, password)
  }

  def insertTable(rdd: RDD[Row], table: String): Unit = {
    rdd.foreachPartition(it => {
      try {
        val conn = JdbcUtil.createConn()
        val ppst = conn.prepareStatement(s"insert into ${table} (" +
          "user_id,interface_no,infotype,user_type,hostname,client,ip,timestamp," +
          "original_logid,original_cmd,type,abnormal,opra_type,use_time,affect," +
          "data_class,table_name,database_name,field_name,origin_content,isViolations," +
          "abnormal_tag,send_to) " +
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
          println(${table} + "插入完成")
          ppst.executeUpdate()
        }
        conn.close()
        ppst.close()
      } catch {
        case e: Exception => print("")
      }
    })

  }

}
