package com.gkgd.bigscreen.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class duridUtils implements Serializable {
    private static DruidDataSource ds;
    //静态代码块，调用该类时候实现仅仅一次
    static {
        try {
            Properties pro = new Properties();
            pro.load(duridUtils.class.getClassLoader()
                    .getResourceAsStream("druid.properties"));
            ds = (DruidDataSource) DruidDataSourceFactory.createDataSource(pro);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static Connection getConn() throws Exception {
        return ds.getConnection();
    }

    public static void closeAll(Connection con, Statement st, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if(st != null){
                st.close();
            }
            if(con != null){
                con.close();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
