package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * hbase操作演示
 * by hechaojie
 */
public class HbaseDemo {
	
	/**
	 * 声明静态配置
	 */
	static Configuration conf = null;
	static Connection conn = null;
	
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.client", "2181");
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)throws Exception {
		
		createTable("users");
//		deleteTable("users");
		
		// 添加数据
//		insertMany();
		// 删除数据
//		deleteData(TableName.valueOf("users"),"rowKey1","base_info","name");
		// 修改数据
//		updateData(TableName.valueOf("users"),"rowKey2","base_info","age","26");
		// 查询数据
//		getResult(TableName.valueOf("users"),"rowKey2");
		// 过滤器
		scanTable(TableName.valueOf("users"));
	}
	
	/**
	 * 创建3个列簇的表
	 */
	public static void createTable(String table) throws Exception {
		Admin admin = conn.getAdmin();
		if (!admin.tableExists(TableName.valueOf(table))) {
			TableName tableName = TableName.valueOf(table);
			//表描述器构造器
			TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
			List<ColumnFamilyDescriptor> columnFamilyDescriptors=new ArrayList<>();
			// 添加3个列族
			columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("base_info")).build());
			columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("ext_info")).build());
			columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("timestamp")).build());
			//添加列族
			tdb.setColumnFamilies(columnFamilyDescriptors);
			//获得表描述器
			TableDescriptor td = tdb.build();
			//创建表
			admin.createTable(td);
		} else {
			System.out.println("表已存在");
		}
		//关闭连接
	}
	
	/**
	 * 删除表
	 */
	public static void deleteTable(String table) throws Exception {
		Admin admin = conn.getAdmin();
		if (admin.tableExists(TableName.valueOf(table))) {
			admin.disableTable(TableName.valueOf(table));
			admin.deleteTable(TableName.valueOf(table));
		}
		//关闭连接
	}
	
	/**
	 * 添加数据（多个rowKey，多个列族）
	 * @throws Exception
	 */
	public static void insertMany() throws Exception{
		Table table = conn.getTable(TableName.valueOf("users"));
		List<Put> puts = new ArrayList<Put>();
		Put put1 = new Put(Bytes.toBytes("rowKey1"));
		put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("wd"));
		
		Put put2 = new Put(Bytes.toBytes("rowKey2"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("23"));
		
		Put put3 = new Put(Bytes.toBytes("rowKey4"));
		put3.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), Bytes.toBytes("男"));
		
		puts.add(put1);
		puts.add(put2);
		puts.add(put3);
		table.put(puts);
		table.close();
	}
	
	/**
	 * 根据rowKey删除一行数据、或者删除某一行的某个列簇，或者某一行某个列簇某列
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteData(TableName tableName, String rowKey, String columnFamily, String columnName) throws Exception{
		Table table = conn.getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		//①根据rowKey删除一行数据
		table.delete(delete);
		
		//②删除某一行的某一个列簇内容
//		delete.addFamily(Bytes.toBytes(columnFamily));
		
		//③删除某一行某个列簇某列的值
//		delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
		table.close();
	}
	
	/**
	 * 根据RowKey , 列簇， 列名修改值
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param columnName
	 * @param columnValue
	 * @throws Exception
	 */
	public static void updateData(TableName tableName, String rowKey, String columnFamily, String columnName, String columnValue) throws Exception{
		Table table = conn.getTable(tableName);
		Put put1 = new Put(Bytes.toBytes(rowKey));
		put1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
		table.put(put1);
		table.close();
	}
	
	/**
	 * 根据rowKey查询数据
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public static void getResult(TableName tableName, String rowKey) throws Exception{
		Table table = conn.getTable(tableName);
		//获得一行
		Get get = new Get(Bytes.toBytes(rowKey));
		Result set = table.get(get);
		Cell[] cells = set.rawCells();
		for (Cell cell: cells){
			System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
					Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
		}
		table.close();
	}

//过滤器 LESS <  LESS_OR_EQUAL <=   EQUAL =   NOT_EQUAL <>   GREATER_OR_EQUAL >=   GREATER >   NO_OP 排除所有
	
	/**
	 * @param tableName
	 * @throws Exception
	 */
	public static void scanTable(TableName tableName) throws Exception{
		Table table = conn.getTable(tableName);
		
		//①全表扫描
//		Scan scan1 = new Scan();
//		ResultScanner rscan = table.getScanner(scan1);
		
		//②rowKey过滤器
//		Scan scan2 = new Scan();
//		//str$ 末尾匹配，相当于sql中的 %str  ^str开头匹配，相当于sql中的str%
//		RowFilter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^rowKey"));
//		scan2.setFilter(filter);
//		ResultScanner resultScanner = table.getScanner(scan2);
		
		
		//③列值过滤器
//		Scan scan3 = new Scan();
//		//下列参数分别为列族，列名，比较符号，值
//		SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("age"),
//				CompareOperator.EQUAL, Bytes.toBytes("26"));
//		// 默认值为false，即如果该行数据不包含参考列，其依然被包含在最后的结果中；设置为true时，则不包含；
//		filter3.setFilterIfMissing(true);
//		scan3.setFilter(filter3);
//		ResultScanner resultScanner = table.getScanner(scan3);
		
		
		//列名前缀过滤器
//		Scan scan4 = new Scan();
//		ColumnPrefixFilter filter4 = new ColumnPrefixFilter(Bytes.toBytes("name"));
//		scan4.setFilter(filter4);
//		ResultScanner resultScanner = table.getScanner(scan4);
		
		
		//过滤器集合
		Scan scan5 = new Scan();
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		SingleColumnValueFilter filter51 = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("name"),
				CompareOperator.EQUAL, Bytes.toBytes("hechaojie"));
		ColumnPrefixFilter filter52 = new ColumnPrefixFilter(Bytes.toBytes("name"));
		list.addFilter(filter51);
		list.addFilter(filter52);
		scan5.setFilter(list);
		ResultScanner resultScanner = table.getScanner(scan5);
		
		for (Result rs : resultScanner){
			String rowKey = Bytes.toString(rs.getRow());
			System.out.println("row key :" + rowKey);
			Cell[] cells = rs.rawCells();
			for (Cell cell: cells){
				System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
						+ Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::"
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
			System.out.println("-------------------------------------------");
		}
	}
}
