package com.atguigu.App;

import com.atguigu.DAO.TagInfoDAO;
import com.atguigu.bean.Taginfo;
import com.atguigu.untils.MyClickhouseUtil;
import com.atguigu.untils.MyPropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author MengX
 * @create 2023/2/16 21:27:08
 */
public class TaskClickhouseOutApp {

    public static void main(String[] args) {


        String taskDate = args[1];

        //saprksession 链接获取

        SparkConf conf = new SparkConf().setAppName("TaskClickhouseOut").setMaster("local[*]");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        //配置文件值获取

        Properties properties = MyPropertiesUtil.load("config.properties");

        String clickhouseUrl = properties.getProperty("clickhouse.url");

        String upDbName = properties.getProperty("user-profile.dbname");


        // 拼接查询语句

        String date = taskDate.replace("-", "");

        String tableName = "tg_merge_profile_"+date;

        String selectSql = "select * from  "+tableName+"";


        // 拼接clickhouse 建表语句

        List<Taginfo> tagInfos = TagInfoDAO.getStatusUpTaginfo();

        List<String> values = tagInfos.stream().map(taginfo -> taginfo.getTagCode().toLowerCase()+" String").collect(Collectors.toList());

        String tableValue = StringUtils.join(values, ",");

        String creatSql = "create table if not exists "+tableName+" (" +
                "uid String,"+tableValue+")" +
                " engine=MergeTree " +
                "order by uid";

        // clickhouse 中建表

        String dropTable = "drop table if exists " + tableName;

//        MyClickhouseUtil.executeSql("use "+upDbName);

        MyClickhouseUtil.executeSql(dropTable);

        MyClickhouseUtil.executeSql(creatSql);

        //从hive中获取数据写入clickhuse

        sparkSession.sql("use "+upDbName);

        Dataset<Row> dataSet = sparkSession.sql(selectSql);

        dataSet.write()
                .mode(SaveMode.Append)
                .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
                //批量提交1.减少连接 网络IO次数 2.减少磁盘碎片 不管是离线还是实时只要对数据库写东西尽量批次的写，不要来一条写一条
                .option("batchsize",500)
                .option("isolationLevel","NONE")  //事务关闭
                .option("numPartitions", "4") // 设置并发
                .jdbc(clickhouseUrl,tableName,new Properties());


        //关闭链接

        sparkSession.close();
    }
}
