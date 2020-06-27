package cn.com.boe.itc.testUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;


public class DelHBaseRows {
	private static Configuration conf;
	private static Connection conn;
	private static Admin admin;
	static {

		conf=HBaseConfiguration.create();
		try {
			conn=ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		if (args == null || args.length <= 0) {
			System.out.println("no arguments");
			return;
		}

		String tableName = args[0].trim();

		String preFix = args[1].trim();

		List<Delete> rows = new ArrayList<Delete>();
//		ArrayList<String> rowkeys = doPrefixFilter(tableName, preFix);
		ArrayList<String> rowkeys = doSubFilter(tableName, preFix);

		for(String rowkey: rowkeys){
			Delete deleteRow = new Delete(Bytes.toBytes(rowkey));
			rows.add(deleteRow);
		}
		deleteRows(tableName, rows);
		close();
	}


	/**
	 * 1、rowkey的前缀过滤器
	 * @param tablename
	 * @param prefixStr
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<String> doPrefixFilter(String tablename, String prefixStr) throws Exception{
		ArrayList<String> rowkeys = new ArrayList<String>();
		Table _table = conn.getTable(TableName.valueOf(tablename));
		Scan scan = new Scan();
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes(prefixStr));
		scan.setFilter(filter);
		ResultScanner resultScanner = _table.getScanner(scan);
		Iterator<Result> its = resultScanner.iterator();
		int i = 0;
		while (its.hasNext()) {
			Result rs = its.next();
			String rowkey = Bytes.toString(rs.getRow());
			rowkeys.add(rowkey);
			System.out.println(rowkey);
			i++;
		}
		System.out.println("Deleted rows: "+i);
		resultScanner.close();
		_table.close();
		return rowkeys;
	}

	/**
	 * 2、rowkey的模糊过滤器
	 * @param tablename
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<String> doSubFilter(String tablename, String sub) throws Exception{
		ArrayList<String> rowkeys = new ArrayList<String>();
		Table _table = conn.getTable(TableName.valueOf(tablename));
		Scan scan = new Scan();
//		PrefixFilter filter = new PrefixFilter(Bytes.toBytes(prefixStr));
		//提取rowkey以包含201407的数据
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(sub));
		scan.setFilter(filter);
		ResultScanner resultScanner = _table.getScanner(scan);
		Iterator<Result> its = resultScanner.iterator();
		int i = 0;
		while (its.hasNext()) {
			Result rs = its.next();
			String rowkey = Bytes.toString(rs.getRow());
			rowkeys.add(rowkey);
			System.out.println(rowkey);
			i++;
		}
		System.out.println("Deleted rows: "+i);
		resultScanner.close();
		_table.close();
		return rowkeys;
	}



	public static void deleteRows(String tablename,List<Delete> deletes) throws Exception{
		Table _table = conn.getTable(TableName.valueOf(tablename));
		for (int i = 0; i < deletes.size(); i++) {
			_table.delete(deletes.get(i));
		}
		System.out.println("Some row deleted successfully！");
	}

	public static void close() throws Exception{
		if(conn!=null){
			conn.close();
		}
		if(admin!=null){
			admin.close();
		}
//		System.out.println("关闭成功");
	}

}
