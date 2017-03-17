package com.exmind.sql;

import java.sql.*;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

/**
 * Description: 数据库连接池类
 * @author 38376
 */
public class ConnectionPool {
    private static Logger log = Logger.getLogger(ConnectionPool.class);
    private static BasicDataSource bs = null;

    /**
     * 创建数据源
     * @return
     */
    public static BasicDataSource getDataSource(String dbUrl, String userName, String password) throws Exception{
        if(bs==null){
            bs = new BasicDataSource();
            bs.setDriverClassName("com.mysql.jdbc.Driver");
            bs.setUrl(dbUrl);
            bs.setUsername(userName);
            bs.setPassword(password);
            bs.setMaxActive(100);//设置最大并发数
            bs.setInitialSize(20);//数据库初始化时，创建的连接个数
            bs.setMinIdle(50);//最小空闲连接数
            bs.setMaxIdle(200);//数据库最大连接数
            bs.setMaxWait(1000);
            bs.setMinEvictableIdleTimeMillis(60*1000);//空闲连接60秒中后释放
            bs.setTimeBetweenEvictionRunsMillis(5*60*1000);//5分钟检测一次是否有死掉的线程
            bs.setTestOnBorrow(true);
        }
        return bs;
    }

    /**
     * 释放数据源
     */
    public static void shutDownDataSource() throws Exception{
        if(bs!=null){
            bs.close();
        }
    }

    /**
     * 获取数据库连接
     * @return
     */
    public static Connection getConnection(String dbUrl, String userName, String password){
        Connection con=null;
        try {
            if(bs!=null){
                con=bs.getConnection();
            }else{
                con=getDataSource(dbUrl, userName, password).getConnection();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return con;
    }

    public static void init(String dbUrl, String userName, String password) {
        try {
            getDataSource(dbUrl, userName, password);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage(), e);
        }
    }

    public static Connection getConnection() {
        Connection con=null;
        if (bs != null){
            try {
                con = bs.getConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return con;
    }

    /**
     * 关闭连接
     */
    public static void closeConn(ResultSet rs, Statement ps, Connection con){
        if(rs!=null){
            try {
                rs.close();
            } catch (Exception e) {
                log.error("关闭结果集ResultSet异常！"+e.getMessage(), e);
            }
        }
        if(ps!=null){
            try {
                ps.close();
            } catch (Exception e) {
                log.error("预编译SQL语句对象Statement关闭异常！"+e.getMessage(), e);
            }
        }
        if(con!=null){
            try {
                con.close();
            } catch (Exception e) {
                log.error("关闭连接对象Connection异常！"+e.getMessage(), e);
            }
        }
    }
}
