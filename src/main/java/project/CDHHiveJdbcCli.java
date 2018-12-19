package project;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Hive的JavaApi
 *
 */
public class CDHHiveJdbcCli {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://zhy.cauchy8389.com:10000/default";
	private static String user = "hive";
	private static String password = "123456";

	public static void main (String[] args) {
		try {

			Class.forName(driverName);
			Connection conn = DriverManager.getConnection(url,user,password);
			Statement stmt = conn.createStatement();

			String sql=" select 20170526 ,20 , count(1) as pv , size(collect_set(user_tracecode ) )  as uv , "
					+ "size(collect_set(user_ip ) )  as IP  , 0 ,0 , 0 ,0 from logtable "
					+ "where day='20170526' and hour='20'";

			ResultSet res = stmt.executeQuery(sql);

			System.out.println("DAY"+ "\t"+ "hour"+ "\t" + "PV"+ "\t" + "UV" + "\t"+ "IP" + "\t"+ "Newuser"
					+ "\t"+ "VisitTimes" + "\t"+ "Avgpv" + "\t"+ "Avgvisittimes");
			Logs logs = new Logs();
			while (res.next()) {
				System.out.println(res.getString(1) + "\t" + res.getString(2)+ "\t" +
						res.getString(3)+ "\t" + res.getString(4)+ "\t" +
						res.getString(5)+ "\t" + res.getString(6)+ "\t" +
						res.getString(7));

				logs.DAY=res.getString(1);

				logs.HOUR=res.getString(2);

				logs.PV=Integer.parseInt(res.getString(3));

				logs.UV=Integer.parseInt(res.getString(4));

				logs.IP=Integer.parseInt(res.getString(5));

				logs.Newuser=Integer.parseInt(res.getString(6));

				logs.VisitTimes=Integer.parseInt(res.getString(7));

				logs.Avgpv=Double.parseDouble(res.getString(8));

				logs.Avgvisittimes=Double.parseDouble(res.getString(9));

				MysqlCli2.insertSql(logs);
			}

			conn.close();
			conn = null;

			System.out.println("Hive操作完毕");

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}  
