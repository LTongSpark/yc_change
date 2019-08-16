package com.zytc.yc.spark.domain

/**
  *
  *
  * @param origin_content        原始日志
  * @param interfaceNo           接口编号 ，用于区分日志来源
  * @param infotype              日志类型loadlog ，authlog ，sensitivelog
  * @param base_userId           系统操作账号id
  * @param base_userType         manual     通过系统人工操作,machine 通过系统接口机器调用
  * @param base_hostname         操作设备主机名
  * @param base_client           操作设备客户端（如IE浏览器、某系统模块名、某客户端等）
  * @param base_ip               操作IP地址
  * @param base_timestamp        系统时间戳
  * @param base_originalLogId    原始日志ID
  * @param base_originalCMD      原始日志操作记录
  * @param operation_type        操作类型
  * @param operation_abnormal    异常情况
  * @param operation_act_do      拿出 insert | delete | update | select | other
  * @param operation_act_useTime 执行时间
  * @param operation_act_affect  影响行数
  * @param optional_dataClass    涉及数据级别，1~4分别对应商密|受限|对内公开|对外公开级
  * @param optional_tablename    涉及表名
  * @param optional_databasename 涉及数据库名
  * @param optional_fieldname    涉及字段名，用“,”分割
  */
case class SensLogInfo(
                          origin_content:String,
                          interfaceNo: String,
                          infotype: String,
                          base_userId: String,
                          base_userType: String,
                          base_hostname: String,
                          base_client: String,
                          base_ip: String,
                          base_timestamp: String,
                          base_originalLogId: String,
                          base_originalCMD: String,
                          operation_type: String,
                          operation_abnormal: String,
                          operation_act_do: String,
                          operation_act_useTime: String,
                          operation_act_affect: String,
                          optional_dataClass: String,
                          optional_tablename: String,
                          optional_databasename: String,
                          optional_fieldname: String
                      )
