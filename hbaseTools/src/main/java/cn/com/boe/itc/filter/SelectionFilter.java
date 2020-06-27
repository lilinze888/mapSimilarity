package cn.com.boe.itc.filter;

import cn.com.boe.itc.comm.HBaseConnection;
import cn.com.boe.itc.pojo.BpmapCorrelation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class SelectionFilter {


    /**
     * 1、获取所有的筛选器
     * rowkey：068|A9A0EPN|TFT2_DR_RANGE_SAT|A180CDN|CD1|766A010123
     *         MD5|OPERATION_ID_EPM|PARAM_NAME_EPM|OPERATION_ID_CRT|PARAM_NAME_CRT|LOT_ID
     * @param tablename
     * @return
     * @throws Exception
     */
    public static Map<String, Set<String>> getSelectionList(String tablename) throws Exception {
        //1.给表--Table赋值
        HBaseConnection conn = new HBaseConnection();
        Table _table = conn.getConnection().getTable(TableName.valueOf(tablename));
        Map<String, Set<String>> selectionList = new LinkedHashMap<>();
        Set<String> modelSet = new HashSet<>();
        Set<String> productionTypeSet = new HashSet<>();
        Set<String> epmOperationSet = new HashSet<>();
        Set<String> epmParamSet = new HashSet<>();
        Set<String> crtOperationSet = new HashSet<>();
        Set<String> crtParamSet = new HashSet<>();

        //2.声明一个扫描 region的对象
        Scan scan = new Scan();
        //3.对整张表加个扫描器，scan扫描之后，返回一个扫描结果集 -->"每个rowkey对应的详细信息"
        ResultScanner resultScanner = _table.getScanner(scan);

        //4.遍历"rowkey", result-->"rowkey的所有单元格cell的集合"
        String[] selectionArr = null;
        for (Result result : resultScanner ) {
            selectionArr = Bytes.toString(result.getRow()).split("\\|");
            epmOperationSet.add(selectionArr[1]);
            epmParamSet.add(selectionArr[2]);
            crtOperationSet.add(selectionArr[3]);
            crtParamSet.add(selectionArr[4]);

            modelSet.add(Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), BpmapCorrelation.MODEL.getBytes())));
            productionTypeSet.add(Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), BpmapCorrelation.PRODUCTIONTYPE.getBytes())));
        }
        selectionList.put(BpmapCorrelation.OPERATION_ID_EPM, epmOperationSet);
        selectionList.put(BpmapCorrelation.PARAM_NAME_EPM, epmParamSet);
        selectionList.put(BpmapCorrelation.OPERATION_ID_CRT, crtOperationSet);
        selectionList.put(BpmapCorrelation.PARAM_NAME_CRT, crtParamSet);
        selectionList.put(BpmapCorrelation.MODEL, modelSet);
        selectionList.put(BpmapCorrelation.PRODUCTIONTYPE, productionTypeSet);

        conn.close();
        return selectionList;
    }

    /**
     * 2、根据传入的字段来返回对应的筛选器
     * rowkey：068|A9A0EPN|TFT2_DR_RANGE_SAT|A180CDN|CD1|766A010123
     * @param tablename
     * @param selection
     * @return
     * @throws Exception
     */
    public static List<String> getSelectionList(String tablename, String selection) throws Exception {
        //1.给表--Table赋值
        HBaseConnection conn = new HBaseConnection();
        Table _table = conn.getConnection().getTable(TableName.valueOf(tablename));
        //2.声明一个扫描 region的对象
        Scan scan = new Scan();
        //3.对整张表加个扫描器，scan扫描之后，返回一个扫描结果集 -->"所有rowkey的集合"
        ResultScanner resultScanner = _table.getScanner(scan);
        //4.将表的扫描结果集放入迭代器中，迭代器中的元素-->"每个rowkey对应的详细信息"
        Iterator<Result> its = resultScanner.iterator();
        //5.遍历"rowkey"
        while (its.hasNext()) {
            //6.迭代器中的元素-->"rowkey的所有单元格cell的集合"
            Result rs = its.next();
            System.out.println(Bytes.toString(rs.getRow()));
        }
        System.out.println("---查询成功！---");
        conn.close();
        return null;
    }


}
