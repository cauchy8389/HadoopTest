package project;

import java.sql.*;

public class MysqlCli2 {

    // 驱动程序名
    static String driver = "com.mysql.jdbc.Driver";

    // URL指向要访问的数据库名scutcs
    static String url = "jdbc:mysql://zhy.cauchy8389.com:3306/yuhui";

    // MySQL配置时的用户名
    static String user = "hive";

    // MySQL配置时的密码
    static String password = "123456";

//    public static void main(String[] args) throws Exception{
//
//    	mysqlCli mysqlCli = new mysqlCli();
//    	Logs logs = new Logs();
//    	logs.DAY="20170527";
//    	logs.HOUR="14";
//    	logs.PV=100;
//    	logs.UV=200;
//    	logs.IP=300;
//    	logs.Newuser=400;
//    	logs.VisitTimes=500;
//    	logs.Avgpv=500.0;
//    	logs.Avgvisittimes=600.0;
//    	mysqlCli2.InsertSql(logs);
//
//    }

    public static void insertSql(Logs log){

        try {

            Connection conn = null ;

            Class.forName(driver);

            conn = DriverManager.getConnection(url, user, password);

            Statement stmt = conn.createStatement();

            String insql="insert into logtable(day,hour,pv,uv,ip,newuser,visittimes,Avgpv,Avgvisittimes) "
                    + "values(?,?,?,?,?,?,?,?,?)";

            PreparedStatement ps=conn.prepareStatement(insql);

            ps.setString(1, log.getDAY());

            ps.setString(2, log.getHOUR());

            ps.setInt(3, log.getPV());

            ps.setInt(4, log.getUV());

            ps.setInt(5, log.getIP());

            ps.setInt(6, log.getNewuser());

            ps.setInt(7, log.getVisitTimes());

            ps.setDouble(8, log.getAvgpv());

            ps.setDouble(9, log.getAvgvisittimes());

            ps.executeUpdate();

            System.out.println("mysql数据插入完毕");

            stmt.close();
            conn.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
