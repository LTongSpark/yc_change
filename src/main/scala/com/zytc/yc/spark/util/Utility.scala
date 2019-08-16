package com.zytc.yc.spark.util

import java.util.Properties
import com.alibaba.fastjson.JSONObject

/**
  * 根据路径中的文件创建一个config对象
  */
object Utility {
    def prop(): Properties = {
        val prop = new Properties()
        prop.setProperty("driver", "com.mysql.jdbc.Driver")
        prop
    }

    /**
      * Json字段解析
      *
      * @param jsonObj      json对象
      * @param fieldList    备选字段集合
      * @param defaultValue 默认值
      * @return
      */
    @annotation.tailrec
    def getJsonStr(jsonObj: JSONObject, fieldList: List[String], defaultValue: Object): Object = {
        var result = jsonObj.getOrDefault(fieldList.head, defaultValue);
        if (result != null && result != "") {
            result
        } else {
            if (fieldList.length < 2) defaultValue
            else getJsonStr(jsonObj, fieldList.tail, defaultValue)
        }
    }
}
