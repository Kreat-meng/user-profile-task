package com.atguigu.App;

import com.atguigu.ConstCodes.ConstCodes;
import com.atguigu.DAO.TagInfoDAO;
import com.atguigu.DAO.TaskTagRuleDAO;
import com.atguigu.DAO.TaskinfoDAO;
import com.atguigu.bean.Taginfo;
import com.atguigu.bean.TaskTagRule;
import com.atguigu.bean.Taskinfo;
import com.atguigu.untils.MyPropertiesUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * @author MengX
 * @create 2023/2/14 19:30:03
 */


public class TaskSqlApp {


    /**
     * 1.获取到taskId和业务日期
     * 2.获取tag_info、task_Info、task_tag_rule这个三张表的数据
     * 3.动态拼写建表语句
     * 4.动态拼写查询语句（基于之前标签任务中定义好的sql，在这个基础上去拼写最终需要的查询语句）
     * 5.拼写插入语句
     * 6.执行
     */


    public static void main(String[] args) {

        //0.创建sparksesion链接

        SparkConf conf = new SparkConf().setAppName("test01");//.setMaster("local[*]");

        SparkSession spss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        //1.获取taskid和业务日期

        String taskId = args[0];
        String taskDate = args[1];

        //拿到hdfs相关配置值

        Properties properties = MyPropertiesUtil.load("config.properties");

        String hdfsPath = properties.getProperty("hdfs-store.path");

        String hbname = properties.getProperty("data-warehouse.dbname");

        String userDbname = properties.getProperty("user-profile.dbname");

        //2.获取 task_info,task_tag_rule,tag_info 三张表数据

        Taskinfo taskinfo = TaskinfoDAO.getTaskinfo(taskId);

        List<TaskTagRule> taskTagRules = TaskTagRuleDAO.getTagtaskrule(taskId);

        Taginfo taginfo = TagInfoDAO.getTaginfo(taskId);

        // 3. 动态拼写建表语句
        /**
         * create table if not exists 表名
         *   (uid String,tag_value 类型)
         *  partitioned by (dt String)
         *   comment ‘xxx'
         *   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
         *   location ‘路径’
         */

        String tableName = taginfo.getTagCode().toLowerCase(Locale.ROOT);

        String comment = taginfo.getTagName();

        String tagValueType = taginfo.getTagValueType();

        String taskSql = taskinfo.getTaskSql();

        String valueType = null;

        if (tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_DATE)){

            valueType = "String";

        }else if (tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_LONG)){

            valueType = "bigint";
        }else if (tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_DECIMAL)){

            valueType = "Decimal(6,2)";
        }else if (tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_STRING)){

            valueType = "String";
        }

    //拼写建表语句

        String createsql = "create table if not exists "+userDbname+"."+tableName + " " +
                "(uid String,tag_value " + valueType + "" +
                ")comment '"+ comment +"'" +
                " partitioned by (dt String)" +
                " row format delimited fields terminated by '\t' " +
                " location '"+hdfsPath+"/"+userDbname+"/"+tableName+"'";



    // 拼写 查询语句

        /*Select
                uid,
        case query_value
        when ‘M’ then ‘男’
        when ‘F’ then ‘女’
        when ‘U’ then ‘未知’
        end as query_value
        from
        (select  id as uid, if(gender<>“”,gender,“U”)  as query_value
        from dim_user_zip where dt=‘9999-12-31’)；*/


        String whenThensql = taskTagRules.stream().map(taskTagRule -> "when '" + taskTagRule.getQueryValue() + "' " +
                "then '" + taskTagRule.getSubTagValue() + "'").reduce(" ", (s, s2) -> s + " " + s2);

        String selcetSql = null;


        if (taskTagRules.size()>0){
            selcetSql = "Select\n" +
                    "                uid,\n" +
                    "        case query_value "+whenThensql+"\n" +
                    "        end as tag_value\n" +
                    "        from\n" +
                    "        ("+taskSql+")";

        }else {
            selcetSql = "Select\n" +
                    "                uid,\n" +
                    "        query_value as tag_value\n" +
                    "        from\n" +
                    "        ("+taskSql+")";
        }


    // 拼写 插入sql

        String insertSql = "insert overwrite "+userDbname+"."+tableName+" partition (dt='"+taskDate+"')" +
                ""+selcetSql+"";


    // 执行语句
        spss.sql("use "+hbname);
        spss.sql(createsql);
        spss.sql(insertSql);


    //关闭资源
        spss.close();

    }



}
