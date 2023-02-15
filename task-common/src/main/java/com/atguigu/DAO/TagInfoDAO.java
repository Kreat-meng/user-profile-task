package com.atguigu.DAO;

import com.atguigu.bean.Taginfo;
import com.atguigu.untils.Mysqluntils;

/**
 * @author MengX
 * @create 2023/2/14 20:57:46
 */
public class TagInfoDAO {

    public static Taginfo getTaginfo(String taskId){

        // 运用工具类 获取 taginfo数据

        String sql = "select * from tag_info where tag_task_id = '"+ taskId + "'";

        Taginfo taginfo = Mysqluntils.queryOne(sql,Taginfo.class,true);

        return taginfo;
    }
}
