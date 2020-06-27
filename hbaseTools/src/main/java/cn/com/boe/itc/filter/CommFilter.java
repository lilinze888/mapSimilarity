package cn.com.boe.itc.filter;

import cn.com.boe.itc.pojo.AvgParamEpm;
import cn.com.boe.itc.pojo.Selection;
import org.apache.hadoop.hbase.filter.*;

/**
 * Created by Lilinze on 2020/6/23.
 */
public class CommFilter {


    public static Filter rowFilter(Selection selection) {
        return new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(
                "|" + selection.getOperation_id_epm()
                        + "|" + selection.getParam_name_epm()
                        + "|" + selection.getOperation_id_crt()
                        + "|" + selection.getParam_name_crt() + "|"));
    }



}
