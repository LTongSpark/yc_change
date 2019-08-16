package com.zytc.yc.spark.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.*;

public class HbaseUtil {
    /**
     * 模糊查询
     *
     * @param regex  正则表达式
     * @return List<String>
     */

    public static List<String> rangeFilterScan(String regex) {
        Table table = null;
        List<String> resultStr = new ArrayList<String>();
        try {
            table = HBaseDriverManager.getConnection().
                    getTable(TableName.valueOf("ns1:yc".getBytes()));
            Scan scan = new Scan();
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex + ".*"));
            scan.setFilter(filter);
            Iterator<Result> it = table.getScanner(scan).iterator();
            while (it.hasNext()) {
                Result r = it.next();
                List<Cell> list = r.listCells();
                for (Cell cell : list) {
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    resultStr.add(value);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultStr;
    }
}
