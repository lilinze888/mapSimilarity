package cn.com.boe.itc.filter;

import cn.com.boe.itc.comm.HBaseConnection;
import cn.com.boe.itc.comm.Utils;
import cn.com.boe.itc.pojo.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class BoxplotFilter {

    /**
     * MD5|OPERATION_ID_EPM|PARAM_NAME_EPM|OPERATION_ID_THK/CD/RS|PARAM_NAME_THK/CD/RS|GLASS_ID
     * 068|A9A0EPN|TFT2_DR_RANGE_SAT|A120TKN|6K_THK3L|761M020080A1
     * @param selection
     * @return
     * @throws Exception
     */
    public static ArrayList<LinkedHashMap<String, String>> queryBoxplotData( Selection selection, String dateFlag) throws Exception {
        //1.给表--Table赋值
        HBaseConnection conn = new HBaseConnection();
        Table _table = conn.getConnection().getTable(TableName.valueOf(AvgParamTotal.TABLE_NAME));
        ArrayList<LinkedHashMap<String, String>> boxplotData = new ArrayList<>();

        //2.声明一个扫描 region的对象
        Scan scan = new Scan();
        scan.setFilter(setFilter(selection, dateFlag));

        //3.对整张表加个扫描器，scan扫描之后，返回一个扫描结果集 -->"每个rowkey对应的详细信息"
        ResultScanner resultScanner = _table.getScanner(scan);

        //4.遍历"rowkey", result-->"rowkey的所有单元格cell的集合"
        for (Result result : resultScanner ) {
            LinkedHashMap<String, String> map = new LinkedHashMap<>();
            map.put(AvgParamTotal.GLASS_ID, Bytes.toString(result.getRow()).split("\\|")[5]);
            map.put(AvgParamTotal.EVENTTIME, Bytes.toString(result.getValue(AvgParamTotal.CF.getBytes(), AvgParamTotal.EVENTTIME.getBytes())));
            map.put(AvgParamTotal.PARAMVALUE_EPMAVG, Bytes.toString(result.getValue(AvgParamTotal.CF.getBytes(), AvgParamTotal.PARAMVALUE_EPMAVG.getBytes())));
            map.put(AvgParamTotal.PARAMVALUE_TC4PAVG, Bytes.toString(result.getValue(AvgParamTotal.CF.getBytes(), AvgParamTotal.PARAMVALUE_TC4PAVG.getBytes())));
            boxplotData.add(map);
        }

        //按每个点的时间排序
        Collections.sort(boxplotData,new Comparator<LinkedHashMap<String,String>>() {
            @Override
            public int compare(LinkedHashMap m1, LinkedHashMap m2) {
                return ((String)m1.get(AvgParamTotal.EVENTTIME)).compareTo((String)m2.get(AvgParamTotal.EVENTTIME));
            }
        });

        conn.close();
        return boxplotData;
    }

    /**
     * MD5|OPERATION_ID_EPM|PARAM_NAME_EPM|OPERATION_ID_THK/CD/RS|PARAM_NAME_THK/CD/RS|GLASS_ID
     * 068|A9A0EPN|TFT2_DR_RANGE_SAT|A120TKN|6K_THK3L|761M020080A1
     * @param selection
     * @return
     */
    public static FilterList setFilter(Selection selection, String dateFlag) {

        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(
                "|" + selection.getOperation_id_epm()
                        +"|" + selection.getParam_name_epm()
                        +"|" + selection.getOperation_id_crt()
                        +"|" + selection.getParam_name_crt() + "|"));

        // 单列过滤
        SingleColumnValueFilter modelFilter = new SingleColumnValueFilter(
                AvgParamTotal.CF.getBytes(),
                AvgParamTotal.MODEL.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                selection.getModel().getBytes());
        // 单列过滤
        SingleColumnValueFilter productionTypeFilter = new SingleColumnValueFilter(
                AvgParamTotal.CF.getBytes(),
                AvgParamTotal.PRODUCTIONTYPE.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                selection.getProductiontype().getBytes());
        // 单列过滤
        SingleColumnValueFilter endTimeLessFilter = new SingleColumnValueFilter(
                AvgParamTotal.CF.getBytes(),
                AvgParamTotal.END_TIME_EPM.getBytes(),
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                selection.getQuerytime().getBytes());
        // 单列过滤
        byte[] lastDate = null;
        if(dateFlag.equalsIgnoreCase("last3months")){
            lastDate = Utils.before3Month(selection.getQuerytime()).getBytes();
        }else if(dateFlag.equalsIgnoreCase("last4weeks")){
            lastDate = Utils.before4Week(selection.getQuerytime()).getBytes();
        }else if(dateFlag.equalsIgnoreCase("last7days")){
            lastDate = Utils.before7Day(selection.getQuerytime()).getBytes();
        }
        SingleColumnValueFilter endTimeGreaterFilter = new SingleColumnValueFilter(
                AvgParamTotal.CF.getBytes(),
                AvgParamTotal.END_TIME_EPM.getBytes(),
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                lastDate);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(rowFilter, endTimeLessFilter, endTimeGreaterFilter, modelFilter, productionTypeFilter));

        return filterList;

    }
//
//
//    /**
//     *  rowkey：068|A9A0EPN|TFT2_DR_RANGE_SAT|2020-01-01 05:11:56|761M9Z0085A1
//     *          MD5|OPERATION_ID|PARAM_NAME|END_TIME|GLASS_ID
//     * @return
//     */
//    public static FilterList setEpmFilter(Selection selection) {
//        Filter operationFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator( selection.getOperation_id_epm()));
//        Filter paramFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator( selection.getParam_name_epm() ));
////        Filter endTimeFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new SubstringComparator(queryTtime));
//
//        // 单列过滤
//        SingleColumnValueFilter modelFilter = new SingleColumnValueFilter(
//                AvgParamEpm.CF.getBytes(),
//                AvgParamEpm.MODEL.getBytes(),
//                CompareFilter.CompareOp.EQUAL,
//                selection.getModel().getBytes());
//        // 单列过滤
//        SingleColumnValueFilter productionTypeFilter = new SingleColumnValueFilter(
//                AvgParamEpm.CF.getBytes(),
//                AvgParamEpm.PRODUCTIONTYPE.getBytes(),
//                CompareFilter.CompareOp.EQUAL,
//                selection.getProductiontype().getBytes());
//        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(operationFilter, paramFilter, modelFilter, productionTypeFilter));
//
//        return filterList;
//    }
//
//    /**
//     * 1、获取AVGPARAM_EPMG表中符合筛选器条件的glass_id的集合
//     * @return
//     * @throws Exception
//     */
//    public static Set<String> getEpmGlassList( Selection selection) throws Exception {
//        //1.给表--Table赋值
//        HBaseConnection conn = new HBaseConnection();
//        Table _table = conn.getConnection().getTable(TableName.valueOf(AvgParamEpm.TABLE_NAME));
//        Set<String> epmGlassList = new HashSet<>();
//
//        //2.声明一个扫描 region的对象
//        Scan scan = new Scan();
//        scan.setFilter(setEpmFilter(selection));
//
//        //3.对整张表加个扫描器，scan扫描之后，返回一个扫描结果集 -->"每个rowkey对应的详细信息"
//        ResultScanner resultScanner = _table.getScanner(scan);
//
//        //4.遍历"rowkey", result-->"rowkey的所有单元格cell的集合"
//        String rk = null;
//        String queryTtime = selection.getQueryTtime();
//        for (Result result : resultScanner ) {
//            rk = Bytes.toString(result.getRow());
//            if( queryTtime.compareTo(rk.split("\\|")[3]) >= 0
//                    && Utils.before3Month(queryTtime).compareTo(rk.split("\\|")[3]) <= 0){
//                epmGlassList.add(rk.split("\\|")[4]);
//            }
//        }
//        conn.close();
//        return epmGlassList;
//    }
//
//    /**
//     * 03a|A420TKN|6K_THK1L|712A9ZD002A4
//     * MD5|OPERATION_ID|PARAM_NAME|GLASS_ID
//     * @param selection
//     * @return
//     */
//    public static FilterList setCrtFilter(Set<String> epmGlassList, Selection selection) {
//        Filter operationFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator( selection.getOperation_id_crt() ));
//        Filter paramFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator( selection.getParam_name_crt() ));
//
//        FilterList glassFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
//        for (String glassId : epmGlassList) {
//            glassFilter.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(glassId)));
//        }
//
//        // 单列过滤
//        SingleColumnValueFilter modelFilter = new SingleColumnValueFilter(
//                AvgParamCrt.CF.getBytes(),
//                AvgParamCrt.MODEL.getBytes(),
//                CompareFilter.CompareOp.EQUAL,
//                selection.getModel().getBytes());
//        // 单列过滤
//        SingleColumnValueFilter productionTypeFilter = new SingleColumnValueFilter(
//                AvgParamCrt.CF.getBytes(),
//                AvgParamCrt.PRODUCTIONTYPE.getBytes(),
//                CompareFilter.CompareOp.EQUAL,
//                selection.getProductiontype().getBytes());
//        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(operationFilter, paramFilter, modelFilter, productionTypeFilter, glassFilter));
//
//        return filterList;
//    }
//
//    /**
//     * 2、获取AVGPARAM_TC4PG表中符合筛选器条件的glass_id的集合
//     * 03a|A420TKN|6K_THK1L|712A9ZD002A4
//     * MD5|OPERATION_ID|PARAM_NAME|GLASS_ID
//     * @return
//     * @throws Exception
//     */
//    public static Set<String> getGlassList( Selection selection) throws Exception {
//        //1.给表--Table赋值
//        HBaseConnection conn = new HBaseConnection();
//        Table _table = conn.getConnection().getTable(TableName.valueOf(AvgParamCrt.TABLE_NAME));
//        Set<String> glassList = new HashSet<>();
//
//        Set<String> epmGlassList = getEpmGlassList(selection);
//        //2.声明一个扫描 region的对象
//        Scan scan = new Scan();
//        scan.setFilter(setCrtFilter(epmGlassList, selection));
//        //3.对整张表加个扫描器，scan扫描之后，返回一个扫描结果集 -->"每个rowkey对应的详细信息"
//        ResultScanner resultScanner = _table.getScanner(scan);
//
//        //4.遍历"rowkey", result-->"rowkey的所有单元格cell的集合"
//        for (Result result : resultScanner ) {
//            glassList.add(Bytes.toString(result.getRow()).split("\\|")[3]);
//        }
//        conn.close();
//        return glassList;
//    }
//
//

//
//
//    /**
//     * 1、获取emp的Boxplot数据
//     * rowkey：068|A9A0EPN|TFT2_DR_RANGE_SAT|2020-01-01 05:11:56|761M9Z0085A1
//     *         MD5|OPERATION_ID|PARAM_NAME|END_TIME|GLASS_ID
//     * @param tablename
//     * @return
//     * @throws Exception
//     */
//    public static Map<String, Set<String>> getSelectionList(String tablename) throws Exception {
//        //1.给表--Table赋值
//        HBaseConnection conn = new HBaseConnection();
//        Table _table = conn.getConnection().getTable(TableName.valueOf(tablename));
//
//        Map<String, Set<String>> selectionList = new LinkedHashMap<>();
//        Set<String> modelSet = new HashSet<>();
//        Set<String> productionTypeSet = new HashSet<>();
//        Set<String> epmOperationSet = new HashSet<>();
//        Set<String> epmParamSet = new HashSet<>();
//        Set<String> crtOperationSet = new HashSet<>();
//        Set<String> crtParamSet = new HashSet<>();
//
//        //2.声明一个扫描 region的对象
//        Scan scan = new Scan();
//        //3.对整张表加个扫描器，scan扫描之后，返回一个扫描结果集 -->"每个rowkey对应的详细信息"
//        ResultScanner resultScanner = _table.getScanner(scan);
//
//        //4.遍历"rowkey", result-->"rowkey的所有单元格cell的集合"
//        String[] selectionArr = null;
//        for (Result result : resultScanner ) {
//            selectionArr = Bytes.toString(result.getRow()).split("\\|");
//            epmOperationSet.add(selectionArr[1]);
//            epmParamSet.add(selectionArr[2]);
//            crtOperationSet.add(selectionArr[3]);
//            crtParamSet.add(selectionArr[4]);
//
//            //1.将rowkey的所有单元格cell放入一个集合中
//            List<Cell> cells = result.listCells();
//            //2.遍历该rowkey对应的所有单元格cell,并获取cell的具体信息(列簇，列名，行键，具体值)
//            for (Cell cell : cells) {
//                if(BpmapCorrelation.MODEL.equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))){
//                    modelSet.add(Bytes.toString(CellUtil.cloneValue(cell)));
//                }
//                if(BpmapCorrelation.PRODUCTIONTYPE.equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))){
//                    productionTypeSet.add(Bytes.toString(CellUtil.cloneValue(cell)));
//                }
//            }
//        }
//        selectionList.put(BpmapCorrelation.OPERATION_ID_EPM, epmOperationSet);
//        selectionList.put(BpmapCorrelation.PARAM_NAME_EPM, epmParamSet);
//        selectionList.put(BpmapCorrelation.OPERATION_ID_CRT, crtOperationSet);
//        selectionList.put(BpmapCorrelation.PARAM_NAME_CRT, crtParamSet);
//        selectionList.put(BpmapCorrelation.MODEL, modelSet);
//        selectionList.put(BpmapCorrelation.PRODUCTIONTYPE, productionTypeSet);
//
//        conn.close();
//        return selectionList;
//    }
//
//    /**
//     * hbase 行键过滤器 RowFilter
//     * @throws Exception
//     */
//    @Test
//    public void rowKeyFilter(String tablename) throws Exception{
//
//        //1.给表--Table赋值
//        HBaseConnection conn = new HBaseConnection();
//        Table _table = conn.getConnection().getTable(TableName.valueOf(tablename));
//
//        Scan scan = new Scan();
//
//        //创建一个过滤器,并将其添加至scan对象   <=
//        RowFilter rowFilter = new RowFilter(LESS, new BinaryComparator(Bytes.toBytes("0003")));
//
//        scan.setFilter(rowFilter);
//
//        // scanner 为 行数据result的集合
//        ResultScanner scanner = mytest1.getScanner(scan);
//
//        for (Result result : scanner) {
//
//            // 获取 rowkey
//            System.out.println("rowkey:"+Bytes.toString(result.getRow()));
//
//            // 指定列族以及列 打印 列 当中的数据出来
//            System.out.println("id:"+Bytes.toInt(result.getValue("f1".getBytes(),"id".getBytes())));
//
//            System.out.println("age:"+Bytes.toInt(result.getValue("f1".getBytes(),"age".getBytes())));
//
//            System.out.println("name:"+Bytes.toString(result.getValue("f1".getBytes(),"name".getBytes())));
//
//        }
//
//        mytest1.close();
//
//        connection.close();
//
//
//    }

    //根据列名范围以及列名前缀过滤数据
/*    public Map<String,List<Cell>> filterByPrefixAndRange(String tableNameString,String colPrefix,
                                                         String minCol,String maxCol) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableNameString));

        //列名前缀匹配
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(colPrefix));

        //列名范围扫描，上下限范围包括
        ColumnRangeFilter rangeFilter = new ColumnRangeFilter(Bytes.toBytes(minCol),true,
                Bytes.toBytes(maxCol),true);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(filter);
        filterList.addFilter(rangeFilter);

        Scan scan = new Scan();
        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
        }
        return map;
    }*/

}
