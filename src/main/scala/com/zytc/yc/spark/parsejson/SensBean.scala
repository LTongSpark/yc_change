package com.zytc.yc.spark.parsejson

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zytc.yc.spark.config.AllFieldList
import com.zytc.yc.spark.domain.SensLogInfo
import com.zytc.yc.spark.util.Utility

/**
  * @author LTong
  * @date 2019-06-21 下午 5:46
  */
object SensBean {
    def operationBean(json: String) = {
        var interfaceNo: String = "" //接口编号，用于区分日志来源
        var infotype: String = "" //日志类型loadlog ，authlog ，sensitivelog
        var base: Object = null //基础信息
        var operation: Object = null //操作信息
        var optional: Object = null //选填字段

        var base_userId: String = "" //系统操作账号id
        var base_userType: String = "" //manual 通过系统人工操作,machine 通过系统接口机器调用
        var base_hostname: String = "" //操作设备主机名
        var base_client: String = "" //操作设备客户端（如IE浏览器、某系统模块名、某客户端等）
        var base_ip: String = "" //操作IP地址
        var base_timestamp: String = "" //系统时间戳
        var base_originalLogId: String = "" //原始日志ID
        var base_originalCMD: String = "" //原始日志操作记录

        var operation_act: Object = null //行为
        var operation_optional: Object = null
        var operation_type: String = "" //操作类型
        var operation_abnormal: String = "" //异常情况

        var operation_act_do: String = "" //拿出 insert | delete | update | select | other
        var operation_act_useTime: String = "" //执行时间
        var operation_act_affect: String = "" //影响行数

        var optional_dataClass: String = "" //涉及数据级别，1~4分别对应商密|受限|对内公开|对外公开级
        var optional_tablename: String = "" //涉及表名
        var optional_databasename: String = "" //涉及数据库名
        var optional_fieldname: String = "" //涉及字段名，用“,”分割
        //do类型
        var doTypeList = List("insert", "select", "update", "delect", "other", "export")
        //infotype类型
        var infotypeTypeList = List("sensitivelog", "apilog")
        try {
            val json_text = JSON.parseObject(json)
            //判断日志类型是否达标
            infotype = Utility.getJsonStr(json_text,
                AllFieldList.fieldMap.get("infoType").get, "").toString
            interfaceNo = Utility.getJsonStr(json_text,
                AllFieldList.fieldMap.get("interfaceNo").get, "").toString
            operation = Utility.getJsonStr(json_text,
                AllFieldList.fieldMap.get("operation").get, null)
            if (operation != null) {
                operation_act = Utility.getJsonStr(operation.asInstanceOf[JSONObject],
                    AllFieldList.fieldMap.get("ope_act").get, null)
            }
            if (operation_act != null) {
                operation_act_do = Utility.getJsonStr(operation_act.asInstanceOf[JSONObject],
                    AllFieldList.fieldMap.get("sens_op_act_do").get, "").toString
            }
            if (infotypeTypeList.contains(infotype.toLowerCase()) || doTypeList.contains(operation_act_do)) {
                //Json尝试解析
                base = Utility.getJsonStr(json_text, AllFieldList.fieldMap.get("base").get, null)
                optional = Utility.getJsonStr(json_text, AllFieldList.fieldMap.get("optional").get, null)

                if (base != null) {
                    base_userId = Utility.getJsonStr(base.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("ba_userId").get, "").toString
                    base_userType = Utility.getJsonStr(base.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("ba_userType").get, "").toString
                    base_hostname = Utility.getJsonStr(base.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("ba_hostname").get, "").toString
                    base_client = Utility.getJsonStr(base.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("ba_client").get, "").toString
                    base_ip = Utility.getJsonStr(base.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("ba_ip").get, "").toString
                    base_timestamp = Utility.getJsonStr(base.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("ba_timestamp").get, "").toString
                    if (base_timestamp.length == 10) base_timestamp = base_timestamp + "000"
                    else if (base_timestamp.length == 11) base_timestamp = base_timestamp + "00"
                    else if (base_timestamp.length == 12) base_timestamp = base_timestamp + "0"
                    else base_timestamp = base_timestamp
                    base_originalCMD = Utility.getJsonStr(base.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("ba_originalCMD").get, "").toString
                        .replaceAll("\\!\\*\\!", "\"")
                    base_originalLogId = Utility.getJsonStr(base.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("ba_originalLogId").get, "").toString

                }

                if (operation != null) {
                    operation_type = Utility.getJsonStr(operation.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("ope_type").get, "").toString
                    operation_abnormal = Utility.getJsonStr(operation.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("ope_abnormal").get, "").toString
                }

                if (optional != null) {
                    optional_dataClass = Utility.getJsonStr(optional.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("opt_class").get, "").toString
                    optional_tablename = Utility.getJsonStr(optional.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("opt_tablename").get, "").toString
                    optional_databasename = Utility.getJsonStr(optional.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("opt_databasename").get, "").toString
                    optional_fieldname = Utility.getJsonStr(optional.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("opt_fieldname").get, "").toString
                }

                if (operation_act != null) {
                    operation_act_affect = Utility.getJsonStr(operation_act.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("sens_op_act_affect").get, "").toString
                    operation_act_useTime = Utility.getJsonStr(operation_act.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("sens_op_act_useTime").get, "").toString
                }
                operation_optional = Utility.getJsonStr(operation.asInstanceOf[JSONObject],
                    AllFieldList.fieldMap.get("optional").get, null)
                if (operation_optional != null) {
                    optional_dataClass = Utility.getJsonStr(operation_optional.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("opt_class").get, "").toString
                    optional_tablename = Utility.getJsonStr(operation_optional.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("opt_tablename").get, "").toString
                    optional_databasename = Utility.getJsonStr(operation_optional.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("opt_databasename").get, "").toString
                    optional_fieldname = Utility.getJsonStr(operation_optional.asInstanceOf[JSONObject],
                        AllFieldList.fieldMap.get("opt_fieldname").get, "").toString
                }
            }
        }
        catch {
            case e: NullPointerException => print("")
            case e: Exception => print("")
        }
        SensLogInfo(json, interfaceNo, infotype, base_userId, base_userType,
            base_hostname, base_client, base_ip, base_timestamp,
            base_originalLogId, base_originalCMD, operation_type,
            operation_abnormal, operation_act_do, operation_act_useTime,
            operation_act_affect, optional_dataClass, optional_tablename,
            optional_databasename, optional_fieldname)
    }
}
