package cn.com.boe.itc.testUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class HBaseTool {
	private static Configuration conf;
	private static Connection conn;
	private static Admin admin; //Hbase中所有对表的管理操作都需要admin对象
	static {
		//1.HBase的configurationg类继承了hadoop的configuration类
		//最终得到的"conf"是在Hadoop的configuration的基础上又添加了HBase的配置项
		conf=HBaseConfiguration.create();
		try {
			//2.连接HBase,定义conn对象
			conn=ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
/************************* 一、create_namespace 表名称空间*************************
	 * 创建名称空间
	 * $>create_namespace 'ns1'
	 */
	public static void createNs(String nameSpace) throws Exception {
		admin = conn.getAdmin();
		NamespaceDescriptor nsDesc=NamespaceDescriptor.create(nameSpace).build();
		admin.createNamespace(nsDesc);
		System.out.println("名称空间"+nameSpace+"创建成功！");
	}
	
	/**
	 * 删除名称空间
	 * $>drop_namespace 'ns1'
	 */
	public static void deleteNs(String nameSpace) throws Exception{
		admin=conn.getAdmin();
		admin.deleteNamespace(nameSpace);
		System.out.println("删除名称空间"+nameSpace+"成功！");
	}
	/**
	 * 查询名称空间列表
	 * $>list_namespace
	 * 查询某个名称空间下的全部表
	 * list_namespace_tables 'default'
	 */
	public static void listNs() throws Exception{
		admin=conn.getAdmin();
		NamespaceDescriptor[] nss=admin.listNamespaceDescriptors();
		System.out.println("-------------------------");
		for (NamespaceDescriptor ns : nss) {
			System.out.println(ns);
		}
		System.out.println("查询名称空间列表成功！");
		System.out.println("-------------------------");
	}
	
/*****************************   二、表Table操作      ***************************/
	/** 列出所有表
	 * $>list
	 * 表级操作(查询，创建，删除表;切分表，切分region;)需要创建admin
	 */
	public static void listTables() throws Exception{
		//获取管理员HMaster的对象
		admin=conn.getAdmin();
		TableName[] tns=admin.listTableNames();
		System.out.println("-------------------------");
		int num = 0;
		for (TableName tn : tns) {
			num++;
			System.out.println(tn);
		}
		System.out.println("共有"+num+"张表，查询所有表成功！");
		System.out.println("-------------------------");
	}
	
	/** 列出某个名称空间下的所有表
	 * $>list
	 * 表级操作(查询，创建，删除表;切分表，切分region;)需要创建admin
	 */
	public static void listTablesByNameSpace(String nameSpace) throws Exception{
		//获取管理员HMaster的对象
		admin=conn.getAdmin();
		TableName[] tns=admin.listTableNamesByNamespace(nameSpace);
		System.out.println("-------------------------");
		for (TableName tn : tns) {
			System.out.println(tn);
		}
		System.out.println("查询所有表成功！");
		System.out.println("-------------------------");
	}
	
	/** 创建表
	 * $>create 'ns1:t1' 'f1'
	 * 只有表级操作(创建，删除表;切分表，切分region;)需要创建admin
	 */
	public static void createTable(String tableName,String columnFamilyName) throws Exception{
		//判断表是否存在  
		if(isTableExist(tableName)){   
			System.out.println("表" + tableName + "已存在");   
			return;
		}
		//获取管理员HMaster的对象
		admin=conn.getAdmin();
		//根据Table名获取表的具体描述对象
		HTableDescriptor htableDesc=new HTableDescriptor(TableName.valueOf(tableName));
		//将列簇加入Table中
		htableDesc.addFamily(new HColumnDescriptor(columnFamilyName));
		admin.createTable(htableDesc);
		System.out.println("创建表"+tableName+"成功！");
	}
	/**
	 * 删除表
	 * $>disable 'ns1:t1'
	 * $>drop 'ns1:student'
	 */
	public static void dropTable(String tableName) throws Exception{
		//判断表是否存在 ,不存在就结束 
		if(!isTableExist(tableName)){   
			System.out.println("表" + tableName + "不存在");   
			return;
		}
		admin=conn.getAdmin();
		TableName _tableName = TableName.valueOf(tableName);
		//先将表禁用
		admin.disableTable(_tableName);
		admin.deleteTable(_tableName);
		System.out.println("删除表"+tableName+"成功！");
	}
	/**
	 * 切分表:按传入的rowkey进行切分，如果不传rowkey就会对半切分Table
	 * $>split 'ns1:t1' 'rowkey'
	 */
	public static void splitTable(String tableName,String rowkey) throws Exception{
		//判断表是否存在  
		if(isTableExist(tableName)){   
			System.out.println("表" + tableName + "已存在");   
			return;
		}
		//获取管理员HMaster的对象
		admin=conn.getAdmin();
		admin.split(TableName.valueOf(tableName), Bytes.toBytes(rowkey));;
		System.out.println("切分"+tableName+"表成功！");
	}
	/**
	 * 切分region:按传入的rowkey进行切分，如果不传rowkey就会对半切分region
	 * 注意：region名是一个MD5码值
	 * $>split 'region的MD5码值' 'rowkey'
	 */
	public static void splitRegion(String regionName,String rowkey) throws Exception{
		//判断表是否存在  
//		if(isTableExist(tableName)){   
//			System.out.println("表" + tableName + "已存在");   
//			return;
//		}
		//获取管理员HMaster的对象
		admin=conn.getAdmin();
		admin.splitRegion(Bytes.toBytes(regionName), Bytes.toBytes(rowkey));
		System.out.println("切分"+regionName+"region成功！");
	}
	/**
	 * 表的批次操作：一次性进行数据的添加，删除，查询操作
	 */
	public static void tableBatch(String tableName,String rowkey) throws Exception{
		//判断表是否存在  
		if(isTableExist(tableName)){   
			System.out.println("表" + tableName + "已存在");   
			return;
		}
		//1.给表--Table赋值
		Table _table = conn.getTable(TableName.valueOf(tableName));
		List<Row> action=new ArrayList<Row>();
		//2.给行键--rowkey赋值
		Put _put=new Put(Bytes.toBytes(rowkey));
		Get _get = new Get(Bytes.toBytes(rowkey));
		Delete _delete=new Delete(Bytes.toBytes(rowkey));
		//3.将增，删，改添加到集合中
		action.add(_put);
		action.add(_get);
		action.add(_delete);
		//4.对增，删，改操作进行批处理
		Object[] obj=_table.batch(action);
		//5.获取批处理的第三个结果
		Result rs =(Result)obj[2];
		//6.打印出rowkey所对应的所有cell结果
		printOut(rs);
		System.out.println("表"+tableName+"批次处理成功！");
	}
	
	/**
	 * 判断表是否存在
	 */
	public static boolean isTableExist(String tableName)  throws Exception{
		admin=conn.getAdmin();
		//将字符串表名转换为Hbase的表对象
		TableName _tableName = TableName.valueOf(tableName);
		//判断表是否存在
		return admin.tableExists(_tableName); 
	}
	
/***************************** 三、 put数据添加操作      ***************************
	 * 单行插入数据版本1--添加一行数据的一个单元格；即每次只能插入一列数据
	 * $>put '名称空间:表名','行键','列簇:列','值'
	 */
	public static void putRowData_1(String tableName,String rowkey,String columnFamily,String column,String value) throws Exception{
		Table _tableName=conn.getTable(TableName.valueOf(tableName));
		//创建对应rowkey的行对象--是查询到的每一行数据-->一个单元格;一个rowkey的全部信息对应多个单元格
		Put put =new Put(Bytes.toBytes(rowkey));
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		_tableName.put(put);
		System.out.println("插入成功！");
	}
	/**
	 * 单行插入数据版本2----优化版：添加一行数据的多个单元格；即每次可在一个列簇下添加多列数据
	 * $>put '名称空间:表名','行键','列簇:列','值'
	 */
	public static void putRowData_2(String tableName,Put put) throws Exception{
		Table _tableName=conn.getTable(TableName.valueOf(tableName));
		_tableName.put(put);
		System.out.println("插入成功！");
	}
	/**
	 * 多行插入数据：传入多个put的集合，遍历集合并循环插入
	 * $>put '名称空间:表名','行键','列簇:列','值'
	 */
	public static void putRowsData(String tablename,List<Put> puts) throws Exception{
		Table _table = conn.getTable(TableName.valueOf(tablename));
		//遍历"行键+单元格"集合，并调用插入单行的方法
		for (int i = 0; i < puts.size(); i++) {
			_table.put(puts.get(i));
		}
		System.out.println("多行数据插入成功！");
	}
	
/***************************** 四、 get数据查询操作      ***************************
	 * 查询单条记录_1：指定行键
	 * $>get '名称空间:表名','行键'
	 */
	public static void getOneRow(String tablename,String rowkey) throws Exception{
		//1.给表--Table赋值
		Table _table = conn.getTable(TableName.valueOf(tablename));
		//2.给行键--rowkey赋值
		Get _get = new Get(Bytes.toBytes(rowkey));
		//3.获取该Table中对应rowkey的所有单元格cell的集合
		Result rs = _table.get(_get);
		//4.将此集合放到List中
		List<Cell> cells = rs.listCells();
		//5.遍历该rowkey对应的所有单元格cell,并获取cell的具体信息(列簇，列名，行键，具体值)
		for (Cell cell : cells) {
			System.out.println("列簇名为："+Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println("列名(限定符)为："+Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println("rowkey为："+Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println("value为："+Bytes.toString(CellUtil.cloneValue(cell)));
		}
		System.out.println("---查询成功！---");
	}
	/**
	 * 查询单条记录_2：指定行键和版本号(时间戳)
	 * $>get '名称空间:表名','行键','时间戳'
	 */
	public static void getOneRow(String tablename,String rowkey,int timestamp) throws Exception{
		//1.给表--Table赋值
		Table _table = conn.getTable(TableName.valueOf(tablename));
		//2.给行键--rowkey赋值
		Get _get = new Get(Bytes.toBytes(rowkey));
		//3.给版本--timestamp赋值
		_get.setMaxVersions(timestamp);
		//4.获取该Table中对应rowkey的所有单元格cell的集合
		Result rs = _table.get(_get);
		//5.将此集合放到List中
		List<Cell> cells = rs.listCells();
		//6.遍历该rowkey对应的所有单元格cell,并获取cell的具体信息(列簇，列名，行键，具体值)
		for (Cell cell : cells) {
			System.out.println("列簇名为："+Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println("列名(限定符)为："+Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println("rowkey为："+Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println("value为："+Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println("timestamp为："+cell.getTimestamp());
		}
		System.out.println("---查询成功！---");
	}
/***************************** 五、 delete数据删除操作      ***************************
	 * 删除数据
	 * $>delete 'customer','row-2','baseinfo:name'
	 */
	public static void deleteOneRow(String tablename,String rowkey) throws Exception{
		//1.给表--Table赋值
		Table _table = conn.getTable(TableName.valueOf(tablename));
		//2.给行键--rowkey赋值
		Delete _delete = new Delete(Bytes.toBytes(rowkey));
		//3.删除该Table中对应rowkey的所有单元格cell的集合
		_table.delete(_delete);
		System.out.println("---删除成功！---");
	}
	
	/**
	 * 删除多行数据：传入多个delete的集合，遍历集合并循环删除
	 * $>delete 'customer','row-2','baseinfo:name'
	 */
	public static void deleteRows(String tablename,List<Delete> deletes) throws Exception{
		//1.给表--Table赋值
		Table _table = conn.getTable(TableName.valueOf(tablename));
		//2.遍历"deletes"集合，并调用删除单行的方法
		for (int i = 0; i < deletes.size(); i++) {
			//3.删除该Table中对应rowkey的所有单元格cell的集合
			_table.delete(deletes.get(i));
		}
		System.out.println("多行数据删除成功！");
	}
/***************************** 六、 scan---查询表数据操作      ***************************
	 * 查询表中所有数据
	 * $>scan 'default:student'
	 */
	public static void scanAll(String tablename) throws Exception{
		//判断表是否存在 ,不存在就结束 
		if(!isTableExist(tablename)){   
			System.out.println("表" + tablename + "不存在");   
			return;
		}
		//1.给表--Table赋值
		Table _table = conn.getTable(TableName.valueOf(tablename));
		//2.声明一个扫描 region的对象 
		Scan scan = new Scan();
		//3.对整张表加个扫描器，scan扫描之后，返回一个扫描结果集 -->"所有rowkey的集合"
		ResultScanner resultScanner = _table.getScanner(scan);
		//4.将表的扫描结果集放入迭代器中，迭代器中的元素-->"每个rowkey对应的详细信息"
		Iterator<Result> its = resultScanner.iterator();
		//5.遍历"rowkey"
		while (its.hasNext()) {
			//6.迭代器中的元素-->"rowkey的所有单元格cell的集合"
			printOut(its.next());
		}
		System.out.println("---查询成功！---");
	}
	
	/*
	 * 1、 查询表中所有rowkey
	 */
	public static void scanAllRowkey(String tablename) throws Exception{
		//1.给表--Table赋值
		Table _table = conn.getTable(TableName.valueOf(tablename));
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
	}
	
	/* 
	 * 2、模糊行键过滤FuzzyRowFilter: 模糊查询所有rowkey
	 * scan 'YIELD:KEY_GENERATOR',LIMIT=>15,FILTER=>"RowFilter(=,'substring:PRCG')"
	 */
	public static void doFuzzyRowFilter(String tablename, String fuzzyRow, byte[] fuzzyNum) throws Exception{
		//1.给表--Table赋值
		Table _table = conn.getTable(TableName.valueOf(tablename));
		//2.声明一个扫描 region的对象 
		Scan scan = new Scan();
		Pair<byte[], byte[]> par1 = new Pair<>(Bytes.toBytes(fuzzyRow), fuzzyNum);
//		Pair<byte[], byte[]> par2 = new Pair<>(Bytes.toBytes("2012"),new byte[]{
//		        0,0,0,0
//		});
		List<Pair<byte[], byte[]>> fuzzy = Arrays.asList(par1);
		
		FuzzyRowFilter filter = new FuzzyRowFilter(fuzzy);
		//2、给scan加上过滤器
		scan.setFilter(filter);
		
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
	}
	
	/* 
	 * 3、前缀行键过滤FuzzyRowFilter: 模糊查询所有rowkey
	 * scan 'YIELD:KEY_GENERATOR',LIMIT=>15,FILTER=>"RowFilter(=,'substring:PRCG')"
	 */
	public static ArrayList<String> doPrefixFilter(String tablename, String prefixStr) throws Exception{
		ArrayList<String> rowkeys = new ArrayList<String>();
		//1.给表--Table赋值
		Table _table = conn.getTable(TableName.valueOf(tablename));
		//2.声明一个扫描 region的对象 
		Scan scan = new Scan();
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes(prefixStr));
		//2、给scan加上过滤器
		scan.setFilter(filter);
		
		//3.对整张表加个扫描器，scan扫描之后，返回一个扫描结果集 -->"所有rowkey的集合"
		ResultScanner resultScanner = _table.getScanner(scan);
		//4.将表的扫描结果集放入迭代器中，迭代器中的元素-->"每个rowkey对应的详细信息"
		Iterator<Result> its = resultScanner.iterator();
		
		//5.遍历"rowkey"
		while (its.hasNext()) {
			//6.迭代器中的元素-->"rowkey的所有单元格cell的集合"
			Result rs = its.next();
			String rowkey = Bytes.toString(rs.getRow());
			rowkeys.add(rowkey);
			System.out.println(rowkey);
		}
		System.out.println("---查询成功！---");
		return rowkeys;
	}
	
	
	/*
	 * 查询表中startkey至endkey之间的数据
	 */
	public static void scanByRowkey(String tablename,String startkey,String endkey) throws Exception{
		//1.给表--Table赋值
		Table _table = conn.getTable(TableName.valueOf(tablename));
		//2.声明一个扫描 region的对象 ,并指定开始和结束的行键
		Scan scan = new Scan(Bytes.toBytes(startkey),Bytes.toBytes(endkey));
		//3.对整张表加个扫描器，scan扫描之后，返回一个扫描结果集 -->"所有rowkey的集合"
		ResultScanner rsscan = _table.getScanner(scan);
		//4.将表的扫描结果集放入迭代器中，迭代器中的元素-->"每个rowkey对应的详细信息"
		Iterator<Result> its = rsscan.iterator();
		//5.遍历"rowkey"
		while (its.hasNext()) {
			//6.迭代器中的元素-->"rowkey的所有单元格cell的集合"
			printOut(its.next());
		}
		System.out.println("---查询成功！---");
	}
	/**
	 * 打印输出
	 * @throws UnsupportedEncodingException 
	 */
	private static void printOut(Result rs) throws UnsupportedEncodingException{
		//1.将rowkey的所有单元格cell放入一个集合中
		List<Cell> cells = rs.listCells();
		//2.遍历该rowkey对应的所有单元格cell,并获取cell的具体信息(列簇，列名，行键，具体值)
		for (Cell cell : cells) {
			String family = Bytes.toString(CellUtil.cloneFamily(cell));
			String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
			String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
			String value = Bytes.toString(CellUtil.cloneValue(cell));
//			String value = new String(CellUtil.cloneValue(cell),"UTF-8");
			long timestamp = cell.getTimestamp();
			System.out.println(rowkey+" column="+family+":"+qualifier+", timestamp="+timestamp+", value="+value);
		}
	}
	
	/**
	 * 关闭HBase数据库连接
	 */
	public static void close() throws Exception{
		if(conn!=null){
			conn.close();
		}
		if(admin!=null){
			admin.close();
		}
		System.out.println("关闭成功");
	}
	
}
