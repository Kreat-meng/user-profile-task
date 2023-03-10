package com.atguigu.untils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author MengX
 * @create 2023/2/16 21:34:42
 */
public class MyClickhouseUtil {

    static Properties properties   = MyPropertiesUtil.load("config.properties");
    static  String  CLICKHOUSE_URL = properties.getProperty("clickhouse.url");

    public static void  executeSql(String sql   ) {
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw  new RuntimeException("找不到驱动");
        }
        try {
            Connection connection = DriverManager.getConnection(CLICKHOUSE_URL, null, null);
            Statement statement = connection.createStatement();
            statement.execute(sql);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw  new RuntimeException("数据库执行失败");
        }

    }
}
