package cn.com.boe.itc.testUtils;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Test;


public class HbaseTest {
	@Test
	public void 查询名称空间() throws Exception {
		HBaseTool.listNs();
		
	}
	@Test
	public void 查询HBase中所有表() throws Exception{
		HBaseTool.listTables();
	}
	@Test
	public void 查询某个名称空间下的中所有表() throws Exception{
		String nameSpace = "default";
		HBaseTool.listTablesByNameSpace(nameSpace);
	}
	
	@Test
	public void 查询某张表中的所有数据() throws Exception{
//		HBaseTool.scanAll("default:student");
//		HBaseTool.scanAll("YIELD:TRACKING_GLASS");
//		HBaseTool.scanAll("SYSTEM.CATALOG");
//		HBaseTool.scanAll("EDATAREALM.EDRINSTANCETREEPATH");
//		HBaseTool.scanAll("YIELD:SV_GLASS_RAW");
		HBaseTool.scanAll("YIELD:test_llz");
//		HBaseTool.scanAll("YIELD:SV_GLASS_RAW");
//		HBaseTool.scanAll("YIELD:AOI_GLASS");
		
		
	}
	
	@Test
	public void 查询某张表中的所有的rowkey() throws Exception{
		HBaseTool.scanAllRowkey("YIELD:AVGPARAM_EPMG");
		
	}
	
	@Test
	public void 模糊查询rowkey() throws Exception{
		HBaseTool.doFuzzyRowFilter("YIELD:KEY_GENERATOR","PRCG",new byte[]{0,0,0,0});
		
	}
	
	@Test
	public void 前缀查询rowkey() throws Exception{
		HBaseTool.doPrefixFilter("YIELD:KEY_GENERATOR","PRCG");
		
	}
	
	
	
	@Test
	public void 创建名称空间() throws Exception{
		HBaseTool.createNs("ns2");
		
	}
	@Test
	public void 删除名称空间() throws Exception{
		HBaseTool.deleteNs("ns2");
		
	}
	@Test
	public void 删除表() throws Exception{
//		HBaseTool.dropTable("llz");
		HBaseTool.dropTable("YIELD:test_llz");
		
		
	}
	@Test
	public void 创建表() throws Exception{
//		HBaseTool.createTable("llz","name");
		HBaseTool.createTable("YIELD:test_llz","C");
		
	}
	@Test
	public void 插入单行数据_1() throws Exception{
//		HBaseTool.putRowData_1("student","row-3","baseinfo","Object","lxy");
		for(int i = 0;i<10;i++){
			HBaseTool.putRowData_1("YIELD:test_llz","zw"+"-"+i,"C","name","3");
		}
	}
	@Test
	public void 插入单行数据_2() throws Exception{
		Put _deleteRow = new Put(Bytes.toBytes("row-4"));
		_deleteRow.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("name"), Bytes.toBytes("abc"));
		_deleteRow.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("age"), Bytes.toBytes("23"));
		_deleteRow.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("class"), Bytes.toBytes("BD1702"));
		HBaseTool.putRowData_2("student",_deleteRow);
		
	}
	@Test
	public void 插入多行数据() throws Exception{
		List<Put> _deletes = new ArrayList<Put>();
		for (int i = 3; i < 10; i++) {
			//定义行键，按行键插入
			Put _delete = new Put(Bytes.toBytes("row-"+i));
			//定义单元格cell的具体值
			_delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("asa"+i));
			_delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes("23"));
			_delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("class"), Bytes.toBytes("BD1702"+i));
			//将多个"行键+单元格"存入到一个集合里
			_deletes.add(_delete);
		}
		HBaseTool.putRowsData("ns1:t1", _deletes);
		
	}
	
	@Test
	public void 查询单条记录() throws Exception{
		HBaseTool.getOneRow("default:student", "row-1");
		
	}
	
	@Test
	public void 查询单条记录通过时间戳() throws Exception{
		HBaseTool.getOneRow("default:student","row-1",000);
		
	}
	
	@Test
	public void 根据startkey和endkey查询表中的数据() throws Exception{
		HBaseTool.scanByRowkey("default:student", "row-1", "row-2");
		
	}
	@Test
	public void 判断表是否存在() throws Exception{
//		boolean flag=HBaseTool.isTableExist("default:student");
		boolean flag=HBaseTool.isTableExist("ns1:student");
		System.out.println(flag);
		
	}
	
	
	@Test
	public void 删除一个rowkey对应的数据() throws Exception{
		HBaseTool.deleteOneRow("default:student","row-1");
	}
	
	@Test
	public void 批量删除模糊rowkey对应的数据() throws Exception{
		List<Delete> rows = new ArrayList<Delete>();
		ArrayList<String> rowkeys = HBaseTool.doPrefixFilter("YIELD:test_llz", "zw");
		for(String rowkey: rowkeys){
			//定义行键，按行键插入
			Delete deleteRow = new Delete(Bytes.toBytes(rowkey));
			//将多个"行键+单元格"存入到一个集合里
			rows.add(deleteRow);
		}
		HBaseTool.deleteRows("YIELD:test_llz", rows);
		
	}
	
	@Test
	public void 批量删除rowkey对应的数据() throws Exception{
		List<Delete> _deletes = new ArrayList<Delete>();
		for (int i = 3; i < 10; i++) {
			//定义行键，按行键插入
			Delete _delete = new Delete(Bytes.toBytes("row-"+i));
			//定义单元格cell的具体值
			_delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));
			_delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"));
			_delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("class"));
			//将多个"行键+单元格"存入到一个集合里
			_deletes.add(_delete);
		}
		HBaseTool.deleteRows("ns1:t1", _deletes);
	}
	
	@Test
	public void 切分表() throws Exception{
		HBaseTool.splitTable("default:student","row-1");
	}
	
	@Test
	public void 切分region() throws Exception{
		long starttime=System.currentTimeMillis();
		HBaseTool.splitRegion("H4BcD5DsB5SdN230","row-1");
		//前后两次时间间隔
		System.out.println(System.currentTimeMillis()-starttime);
	}
	
	
	//每次单元测试都关闭连接
	@After
	public void close() throws Exception{
		HBaseTool.close();
	}
	
	
	
	
	
}
