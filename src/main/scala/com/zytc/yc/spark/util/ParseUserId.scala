package com.zytc.yc.spark.util

object ParseUserId {
    /**
      * 解析userid字段
      * @param userid
      * @return
      */
    def parse(userid:String): Tuple2[String,String] ={
        var userId: String = ""
        var sendTo: String = ""
        if (userid.equals("")) userId = userid
        else if (userid.contains(":")) {
            userId = userid.substring(0, userid.indexOf(":"))
            sendTo = userid.substring(userid.indexOf(":") + 1)
        }
        else if (!userid.contains(",")) userId = userid
        else {
            userId = userid.substring(0, userid.indexOf(","))
            sendTo = userid.substring(userid.indexOf(",") + 1)
        }
        (userId,sendTo)
    }
}
