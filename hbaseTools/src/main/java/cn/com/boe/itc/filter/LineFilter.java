package cn.com.boe.itc.filter;

import cn.com.boe.itc.comm.HBaseConnection;
import cn.com.boe.itc.pojo.BpmapCorrelation;
import cn.com.boe.itc.pojo.Selection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LineFilter {

    static final Logger logger = LoggerFactory.getLogger(LineFilter.class);

    /**
     * 1、获取AVGPARAM_EPMG表中符合筛选器条件的glass_id的集合
     *  rowkey：068|A9A0EPN|TFT2_SFACTOR_SAT|AC30RSN|VALUE|766A010123
     *          MD5|OPERATION_ID_EPM|PARAM_NAME_EPM|OPERATION_ID_CRT|PARAM_NAME_CRT|LOT_ID
     * @return
     * @throws Exception
     */
    public static ArrayList<LinkedHashMap<String, String>> queryLineData(Selection selection) throws Exception {
        HBaseConnection conn = new HBaseConnection();
        Table _table = conn.getConnection().getTable(TableName.valueOf(BpmapCorrelation.TABLE_NAME));
        ArrayList<LinkedHashMap<String, String>> lineData = new ArrayList<>();

        Scan scan = new Scan();
        scan.setFilter(setLineFilter(selection));
        ResultScanner resultScanner = _table.getScanner(scan);

        logger.info("line===" + selection);

        for (Result result : resultScanner ) {
            LinkedHashMap<String, String> map = new LinkedHashMap<>();
            map.put(BpmapCorrelation.LOT_ID, Bytes.toString(result.getRow()).split("\\|")[5]);
            map.put(BpmapCorrelation.EVENTTIME, Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), BpmapCorrelation.EVENTTIME.getBytes())));
            map.put(BpmapCorrelation.PROCESSOPERATION_ID, Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), BpmapCorrelation.PROCESSOPERATION_ID.getBytes())));
            map.put(BpmapCorrelation.R, Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), BpmapCorrelation.R.getBytes())));
            map.put(BpmapCorrelation.PARAM_VALUE_AVG, Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), BpmapCorrelation.PARAM_VALUE_AVG.getBytes())));
            lineData.add(map);
        }

        //按每个点的时间排序
        Collections.sort(lineData,new Comparator<LinkedHashMap<String,String>>() {
            @Override
            public int compare(LinkedHashMap m1, LinkedHashMap m2) {
                return ((String)m1.get(BpmapCorrelation.EVENTTIME)).compareTo((String)m2.get(BpmapCorrelation.EVENTTIME));
            }
        });

        conn.close();
        return lineData;
    }


    /**
     *  rowkey：068|A9A0EPN|TFT2_SFACTOR_SAT|AC30RSN|VALUE|766A010123
     *          MD5|OPERATION_ID_EPM|PARAM_NAME_EPM|OPERATION_ID_CRT|PARAM_NAME_CRT|LOT_ID
     * @return
     */
    public static FilterList setLineFilter(Selection selection) {
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(
                "|" + selection.getOperation_id_epm()
                        +"|" + selection.getParam_name_epm()
                        +"|" + selection.getOperation_id_crt()
                        +"|" + selection.getParam_name_crt() + "|"));

        // 单列过滤
        SingleColumnValueFilter modelFilter = new SingleColumnValueFilter(
                BpmapCorrelation.CF.getBytes(),
                BpmapCorrelation.MODEL.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                selection.getModel().getBytes());
        // 单列过滤
        SingleColumnValueFilter productionTypeFilter = new SingleColumnValueFilter(
                BpmapCorrelation.CF.getBytes(),
                BpmapCorrelation.PRODUCTIONTYPE.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                selection.getProductiontype().getBytes());
        // 单列过滤
        SingleColumnValueFilter endTimeFilter = new SingleColumnValueFilter(
                BpmapCorrelation.CF.getBytes(),
                BpmapCorrelation.END_TIME.getBytes(),
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                selection.getQuerytime().getBytes());

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                Arrays.asList(rowFilter, endTimeFilter, modelFilter, productionTypeFilter));

        return filterList;
    }

/*
    public static ArrayList<LinkedHashMap<String,String>> queryLine2Data(Selection selection) throws Exception {
        HBaseConnection conn = new HBaseConnection();
        Table _table = conn.getConnection().getTable(TableName.valueOf(BpmapCorrelation.TABLE_NAME));
        ArrayList<LinkedHashMap<String, String>> lineData = new ArrayList<>();
        ArrayList<String> lotIdList = new ArrayList<>();

        Scan scan = new Scan();
        scan.setFilter(setLine2Filter(selection));
        ResultScanner resultScanner = _table.getScanner(scan);

        logger.info("line2===" + selection);
        String lotId = null;
        for (Result result : resultScanner ) {
            lotId = Bytes.toString(result.getRow()).split("\\|")[5];
            if(!lotIdList.contains(lotId)){
                LinkedHashMap<String, String> map = new LinkedHashMap<>();
                map.put(BpmapCorrelation.LOT_ID, lotId);
                map.put(BpmapCorrelation.EVENTTIME, Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), BpmapCorrelation.EVENTTIME.getBytes())));
                map.put(BpmapCorrelation.PROCESSOPERATION_ID, Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), BpmapCorrelation.PROCESSOPERATION_ID.getBytes())));
                map.put(BpmapCorrelation.PARAM_VALUE_AVG, Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), BpmapCorrelation.PARAM_VALUE_AVG.getBytes())));
                lotIdList.add(lotId);
                lineData.add(map);
            }
        }
        conn.close();
        return lineData;

    }

    *//**
     *  rowkey：068|A9A0EPN|TFT2_SFACTOR_SAT|AC30RSN|VALUE|766A010123
     *          MD5|OPERATION_ID_EPM|PARAM_NAME_EPM|OPERATION_ID_CRT|PARAM_NAME_CRT|LOT_ID
     * @return
     *//*
    public static FilterList setLine2Filter(Selection selection) {

        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(
                "|" + selection.getOperation_id_crt() +  "|" + selection.getParam_name_crt() + "|" ));

        // 单列过滤
        SingleColumnValueFilter modelFilter = new SingleColumnValueFilter(
                BpmapCorrelation.CF.getBytes(),
                BpmapCorrelation.MODEL.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                selection.getModel().getBytes());
        // 单列过滤
        SingleColumnValueFilter productionTypeFilter = new SingleColumnValueFilter(
                BpmapCorrelation.CF.getBytes(),
                BpmapCorrelation.PRODUCTIONTYPE.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                selection.getProductiontype().getBytes());
        // 单列过滤
        SingleColumnValueFilter endTimeFilter = new SingleColumnValueFilter(
                BpmapCorrelation.CF.getBytes(),
                BpmapCorrelation.END_TIME.getBytes(),
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                selection.getQuerytime().getBytes());

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                Arrays.asList( rowFilter, endTimeFilter, modelFilter, productionTypeFilter));

        return filterList;
    }*/


}
