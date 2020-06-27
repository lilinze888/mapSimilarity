package cn.com.boe.itc.filter;

import cn.com.boe.itc.comm.HBaseConnection;
import cn.com.boe.itc.pojo.BpmapCorrelation;
import cn.com.boe.itc.pojo.CoordinateMaplot;
import cn.com.boe.itc.pojo.Selection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class ContourFilter {


    /**
     *  rowkey：MD5|OPERATION_ID|PARAM_NAME|LOT_ID
     *          03a|A420TKN|6K_THK1L|761M010095
     * @return
     * @throws Exception
     */
    public static LinkedHashMap<String, List> queryData(String lotId, String operationId, String paramName) throws Exception {
        HBaseConnection conn = new HBaseConnection();
        Table _table = conn.getConnection().getTable(TableName.valueOf(CoordinateMaplot.TABLE_NAME));
        LinkedHashMap<String, List> map = new LinkedHashMap<>();

        Scan scan = new Scan();
        scan.setFilter(setFilter(lotId, operationId, paramName));
        ResultScanner resultScanner = _table.getScanner(scan);

        for (Result result : resultScanner ) {

            map.put(CoordinateMaplot.ARRAY_X,
                    Arrays.asList(Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), CoordinateMaplot.ARRAY_X.getBytes())).split("\\|")));
            map.put(CoordinateMaplot.ARRAY_Y,
                    Arrays.asList(Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), CoordinateMaplot.ARRAY_Y.getBytes())).split("\\|")));
            map.put(CoordinateMaplot.ARRAY_Z,
                    Arrays.asList(Bytes.toString(result.getValue(BpmapCorrelation.CF.getBytes(), CoordinateMaplot.ARRAY_Z.getBytes())).split("\\|")));
        }
        conn.close();
        return map;
    }


    public static FilterList setFilter(String lotId, String operationId, String paramName) {
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator( "|" + operationId + "|" + paramName + "|" + lotId ));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                Arrays.asList(rowFilter));

        return filterList;
    }



}
