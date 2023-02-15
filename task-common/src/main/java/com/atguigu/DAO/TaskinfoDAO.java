package com.atguigu.DAO;

import com.atguigu.bean.Taskinfo;
import com.atguigu.untils.Mysqluntils;

/**
 * @author MengX
 * @create 2023/2/14 19:27:38
 */
public class TaskinfoDAO {

    public static Taskinfo getTaskinfo(String taskId){

        //调用 mysql工具类 获取数据，并封装进javabean中

        String sql = "select * from task_info where id = '"+ taskId +"'";

        Taskinfo taskinfo = Mysqluntils.queryOne(sql, Taskinfo.class, true);

        return taskinfo;

    }
}
