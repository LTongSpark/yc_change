package com.zytc.yc.spark.config

object AllFieldList {
    var fieldMap: Map[String, List[String]] = Map(
        /**
          * 公共字段
          */
        "infoType" -> List("infotype", "Infotype", "infoType"),
        "interfaceNo" -> List("interfaceNo", "interfaceId", "interfaceid", "interface", "interfaceno", "interfaceNO", "Interface"),
        "base" -> List("base", "Base"),
        "operation" -> List("operation", "Operation"),
        "optional" -> List("optional", "Optional"),

        "ba_userId" -> List("userId", "userid"),
        "ba_userType" -> List("userType", "usertype", "UserType", "Usertype"),
        "ba_hostname" -> List("hostname", "hostName"),
        "ba_client" -> List("client", "Client"),
        "ba_ip" -> List("ip", "Ip"),
        "ba_timestamp" -> List("timestamp", "Timestamp"),
        "ba_originalCMD" -> List("originalcmd", "originalCMD", "OriginalCMD", "originalcmd"),
        "ba_originalLogId" -> List("originallogid", "originalLogId"),

        "ope_type" -> List("type", "Type"),
        "ope_act" -> List("act", "Act"),
        "ope_abnormal" -> List("abnormal", "Abnormal"),

        "opt_class" -> List("Class", "dataClass", "class", "DataClass", "dataclass", "Dataclass"),
        "opt_tablename" -> List("tablename", "tbname", "Tablename", "tableName", "tbName", "TBName"),
        "opt_databasename" -> List("dbname", "databasename", "Databasename", "databaseName", "dbName"),
        "opt_fieldname" -> List("fieldname", "fdname", "fdName", "Fieldname"),

        /**
          *授权日志字段
          */
        "auth_op_act_do" -> List("do", "doType", "action", "Do", "dotype", "DoType", "Dotype", "Action"),
        "auth_op_act_targetRoleIds" -> List("targetRoleIds", "targetroleids"),
        "auth_op_act_targetUserId" -> List("targetUserId", "TargetUserId", "targetuserId", "targetuserid", "TargetuserId"),
        "auth_op_act_original" -> List("original", "Original"),
        "auth_op_act_now" -> List("now", "Now"),

        /**
          * 登录日志字段
          */
        "login_op_act_do" -> List("do", "doType", "action", "Do", "dotype", "DoType", "Dotype", "Action"),
        "login_op_act_result" -> List("result", "Result"),

        /**
          * 操作日志字段
          */
        "sens_op_act_do" -> List("do", "doType", "action", "Do", "dotype", "DoType", "Dotype", "Action"),
        "sens_op_act_useTime" -> List("usetime", "useTime"),
        "sens_op_act_affect" -> List("affect", "Affect")
    )

}
