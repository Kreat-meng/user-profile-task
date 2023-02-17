package com.atguigu.App;

import com.atguigu.DAO.TagInfoDAO;
import com.atguigu.DAO.TaskinfoDAO;
import com.atguigu.bean.Taginfo;
import com.atguigu.untils.MyPropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author MengX
 * @create 2023/2/16 11:29:00
 */
public class TaskMergeApp {

    public static void main(String[] args) {


        //创建sparksession链接

        SparkConf conf = new SparkConf().setAppName("TaskMergeApp");//.setMaster("local[*]");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        String taskDate = args[1];

        //获取hive中相关库地址

        Properties properties = MyPropertiesUtil.load("config.properties");

        String hdfsPath = properties.getProperty("hdfs-store.path");

        String upDbname = properties.getProperty("user-profile.dbname");

        // 获取标签任务为开启状态的 标签表名

        List<Taginfo> tagInfos = TagInfoDAO.getStatusUpTaginfo();

        List<String> unionSqls = tagInfos.stream().map(taginfo -> "select uid,cast(tag_value as String)," +
                "'"+taginfo.getTagCode().toLowerCase()+"' as tag_code from "+taginfo.getTagCode().toLowerCase()+" "+
                "where dt = '" + taskDate + "'").collect(Collectors.toList());

        String unionSql = StringUtils.join(unionSqls, " union ");

        String date = taskDate.replace("-", "");

        List<String> values = tagInfos.stream().map(taginfo -> taginfo.getTagCode().toLowerCase()+" String").collect(Collectors.toList());

        List<String> values1 = tagInfos.stream().map(taginfo -> taginfo.getTagCode().toLowerCase()).collect(Collectors.toList());

        String value1 = StringUtils.join(values1, "','");

        String tableValue = StringUtils.join(values, ",");

        String tableName = "tg_merge_profile_"+date;

        String dropTableSQL = "drop table if exists " +tableName;


        //创建hive库里面的宽表

        /**
         *  create table if not exists up_tag_merge_20200614
         *   (uid String,
         *     tg_person_base_gender string,
         *     tg_person_base_agegroup string)
         *   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
         *   location 'hdfs://hadoop102:8020/user_profile/user_profile/up_tag_merge_20200614'
         */

        String createSql = "create table if not exists "+tableName+"(" +
                "uid String,"+tableValue+") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'" +
                " location '"+hdfsPath+"/"+upDbname+"/"+tableName+"'";

        //System.out.println(createSql);
        //拼接查询sql

        /**
         * select * from tablename pivot ( sum(聚合列) as 列标识  for 旋转列 in( 旋转列值1 ,旋转列值2,旋转列值3) )
         */

        String selectSql = "select * from ("+unionSql+") pivot ( max(tag_value) as tag_value for tag_code in('"+value1+"'))";

        //System.out.println(selectSql);
        //插入数据

        String insertSql = "insert overwrite table "+tableName+" "+selectSql+"";

        //System.out.println(insertSql);
        // 执行
        sparkSession.sql("use "+upDbname);

        sparkSession.sql(dropTableSQL);

        sparkSession.sql(createSql);

        sparkSession.sql(insertSql);

        //关闭链接

        sparkSession.close();
    }
}
