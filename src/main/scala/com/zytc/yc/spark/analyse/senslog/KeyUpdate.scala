package com.zytc.yc.spark.analyse.senslog

import com.typesafe.config.Config
import com.zytc.yc.spark.parsejson.SensBean
import com.zytc.yc.spark.util.{DateUtil, Utility}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream

/**
  * @author LTong
  * @date 2019-06-21 下午 5:45
  */
object KeyUpdate {
    def key(config: Config, streamData: InputDStream[ConsumerRecord[String, String]], mysqlStatisticsUrl: String): Unit = {
        val input_url = config.getString("mysql.ycdata_url")
        val prop = Utility.prop()
        val spark = SparkSession.builder.config(new SparkConf()).getOrCreate()
        val key = spark.read.jdbc(input_url, "biz_log_keyobject", prop)
        key.createOrReplaceTempView("key_object")
        val userId = spark.sql("select user_id from key_object where user_type = 3")
        val key_object = userId.rdd.map(x => (x.getLong(0).toString, true))
        val json = streamData.map(_.value()).map(rdd => SensBean.operationBean(rdd))
            .filter(x => x.base_userId.length > 0 && x.operation_act_do == "update" && x.operation_abnormal.length > 0)
            .map(x => (x.base_userId, (DateUtil.DateTimeHH(x.base_timestamp)  , x.operation_act_do)))

        json.transform(x => {
            x.leftOuterJoin(key_object)
        }).filter(x => (x._2._2.getOrElse(false) == true))
            .map(x => (x._1, x._2._1._1, x._2._1._2))
            .foreachRDD(rdd => {
                import spark.implicits._
                val key = rdd.toDF("userId", "timestamp", "Do")
                val wcKey = key.groupBy("userId", "timestamp", "Do")
                    .count()
                    .sort($"timestamp".desc)
                    .sort($"count".desc)
                wcKey.write.mode(SaveMode.Append).jdbc(mysqlStatisticsUrl, "biz_log_keyUpdate", prop)
            })
    }

}
