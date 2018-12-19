package bigdata.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
/**
 *
 * ClassName: HbaseUtil 
 * @Description: Hbase交互工具类
 * @author zhy
 * @date 2018-11-20
 */
public class HbaseDao {

	public static Logger log = Logger.getLogger(HbaseDao.class);

	public static Configuration configuration;
	public static Connection conn;
	static {
		System.setProperty("hadoop.home.dir",
				"E:\\hadoop-2.6.0-cdh5.8.5");

		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "zhy.cauchy8389.com");
		//configuration.set("hbase.zookeeper.quorum", "tagtic-master,tagtic-slave02,tagtic-slave03");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("zookeeper.znode.parent", "/hbase");

		try {
			conn = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

//		createTable("hadoop:bigtable");
//    	 dropTable("hadoop:bigtable");
//
		 insertData("xiaoming" , "hadoop:bigtable");
//
//		 QueryAll("YH_BigData");
//		 QueryByCondition1("xiaohui",  "YH_BigData");
//		 QueryByCondition1("xiaoming",  "YH_BigData");
//
//		deleteRow("YH_BigData","xiaoming");

	}

		/**
		 * 创建表
		 * @param tableName
		 */
	public static void createTable(String tableName) {
		log.info("start create table ......");
		try {
			Admin hBaseAdmin =  conn.getAdmin();

			//HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
			TableName tn = TableName.valueOf(tableName);
			if (hBaseAdmin.tableExists(tn)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tn);
				hBaseAdmin.deleteTable(tn);
				log.info("-------------->"+tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
			tableDescriptor.addFamily(new HColumnDescriptor("column1"));
			tableDescriptor.addFamily(new HColumnDescriptor("column2"));
			tableDescriptor.addFamily(new HColumnDescriptor("column3"));
			hBaseAdmin.createTable(tableDescriptor);

		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		log.info("end create table ......");
	}

	/**
	 * 插入数据
	 * @param tableName
	 */
	private static void insertData(String rowkey , String tableName) {
		try {
			Table ht = conn.getTable(TableName.valueOf(tableName));
			// HTable ht = new HTable(configuration, tableName);

			//RowKey
			Put put = new Put(rowkey.getBytes());

			//          列族                                        列名           值
			put.addColumn(Bytes.toBytes("column2"), "screen_width".getBytes(), "1080".getBytes());// 本行数据的第一列
			put.addColumn(Bytes.toBytes("column2"), "screen_height".getBytes(), "1920".getBytes());// 本行数据的第三列
			put.addColumn(Bytes.toBytes("column2"), "url".getBytes(), "www.baidu.com".getBytes());// 本行数据的第三列
			put.addColumn(Bytes.toBytes("column2"), "event_data".getBytes(), "12|16|13|17|12|16".getBytes());// 本行数据的第四列

			put.setDurability(Durability.SYNC_WAL);

			//put a record
			ht.put(put);
			ht.close();
		} catch (IOException e) {
			e.printStackTrace();
		}



	}

	/**
	 * 删除一张表
	 * @param tableName
	 */
	public static void dropTable(String tableName) {
		try {
			Admin admin = conn.getAdmin();
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
			log.info("-------------->"+tableName + " is exist,detele....");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	/**
	 * 根据 rowkey删除一条记录
	 * @param tableName
	 * @param rowKey
	 */
	public static void deleteRow(String tableName, String rowKey)  {
		try {
			//HTable table = new HTable(configuration, tablename);
			Table table = conn.getTable(TableName.valueOf(tableName));
			List list = new ArrayList();
			Delete d1 = new Delete(rowKey.getBytes());
			list.add(d1);

			table.delete(list);
			System.out.println("-------------->"+"删除行成功!");

		} catch (IOException e) {
			e.printStackTrace();
		}


	}


	/**
	 * 查询所有数据
	 * @param tableName
	 */
	public static void queryAll(String tableName) {
		try {
			//HTable table = new HTable(configuration, tableName);
			Table table = conn.getTable(TableName.valueOf(tableName));

			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily())
							+ "====值:" + new String(keyValue.getValue()));
				}


				System.out.println("____toString____" );
				System.out.println(r.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 单条件查询,根据rowkey查询唯一一条记录
	 * @param tableName
	 */
	public static void queryByCondition1(String rowkey , String tableName) {

		try {
			//HTable table = new HTable(configuration, tableName);
			Table table = conn.getTable(TableName.valueOf(tableName));
			Get scan = new Get(rowkey.getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				System.out.println("列：" + new String(keyValue.getFamily())
						+ "====值:" + new String(keyValue.getValue()));
			}

			System.out.println("____toString____" );
			System.out.println(r.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}