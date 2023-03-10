package com.atguigu.untils;

import com.atguigu.untils.MyPropertiesUtil;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author MengX
 * @create 2023/2/14 12:01:45
 */
public class Mysqluntils {

    static Properties properties = MyPropertiesUtil.load("config.properties");


    static  String MYSQL_URL = properties.getProperty("mysql.url");
    static String MYSQL_USERNAME = properties.getProperty("mysql.username");
    static String MYSQL_PASSWORD = properties.getProperty("mysql.password");


    public static  <T> List<T> queryList(String sql, Class<T> clazz, Boolean underScoreToCamel ) {
        try {

            Class.forName("com.mysql.jdbc.Driver");
            List<T> resultList  = new ArrayList<T>();
            Connection conn   = DriverManager.getConnection(MYSQL_URL,MYSQL_USERNAME,MYSQL_PASSWORD);
            Statement stat   = conn.createStatement();
            ResultSet rs   = stat.executeQuery(sql );
            ResultSetMetaData md  = rs.getMetaData();
            while (  rs.next() ) {
                T obj = clazz.newInstance();
                for (int i=1;  i<= md.getColumnCount() ;i++  ) {
                    String propertyName;
                    if(underScoreToCamel) {
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,md.getColumnLabel(i));
                    }else{
                        propertyName =md.getColumnLabel(i);
                    }
                    if(rs.getObject(i)!=null){
                        BeanUtils.setProperty(obj,propertyName, rs.getObject(i));
                    }

                }
                resultList.add(obj);
            }

            stat.close();
            conn.close();
            return  resultList ;

        } catch ( Exception e) {
            e.printStackTrace();
            throw  new RuntimeException("查询mysql失败！");
        }
    }

    public static    <T>  T  queryOne(String sql,Class<T> clazz,Boolean underScoreToCamel ) {
        List<T> queryList = queryList(sql, clazz, underScoreToCamel);
        if(queryList.size()>0){
            return  queryList.get(0);
        }else{
            return null;
        }
    }

}


