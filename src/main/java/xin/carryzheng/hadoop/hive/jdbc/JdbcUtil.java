package xin.carryzheng.hadoop.hive.jdbc;

import java.sql.*;


public class JdbcUtil {

    /**
     * @Field: pool
     * 数据库连接池
     */
    private static JdbcPool pool = new JdbcPool();


    /**
     * 获取数据库连接
     * @author zhengxin
     * @date   2019/4/24 16:57
     */
    private static Connection getConnection() throws SQLException {
        return pool.getConnection();
    }


    /**
     * 查询数据库列表
     * @author zhengxin
     * @date   2019/4/24 17:03
     */
    public static void showDatabases(){

        try (Connection conn = JdbcUtil.getConnection();
             ResultSet rs = conn.prepareStatement("show databases").executeQuery();){

            while(rs.next()){
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
