package com.atguigu.App;

import com.atguigu.ConstCodes.ConstCodes;
import com.atguigu.DAO.TagInfoDAO;
import com.atguigu.bean.Taginfo;
import com.atguigu.untils.MyClickhouseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author MengX
 * @create 2023/2/17 18:18:33
 */
public class TaskBigmapOnClickhouseApp {

    public static void main(String[] args) {

        String taskDate = args[1];

        //创建sparksession链接

        SparkConf conf = new SparkConf().setAppName("bitmapTable");//.setMaster("local[*]");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();


        //创建筛选字段集合

        List<Taginfo> taskListLong = new ArrayList<>();
        List<Taginfo> taskListString = new ArrayList<>();
        List<Taginfo> taskListDecimal = new ArrayList<>();
        List<Taginfo> taskListDate = new ArrayList<>();


        //筛选对应字段类型集合

        List<Taginfo> taginfos = TagInfoDAO.getStatusUpTaginfo();


        for (Taginfo taginfo : taginfos) {

            if (taginfo.getTagValueType().equals(ConstCodes.TAG_VALUE_TYPE_STRING)){

                taskListString.add(taginfo);

            }else if (taginfo.getTagValueType().equals(ConstCodes.TAG_VALUE_TYPE_LONG)){

                taskListLong.add(taginfo);

            }else if (taginfo.getTagValueType().equals(ConstCodes.TAG_VALUE_TYPE_DATE)){

                taskListDate.add(taginfo);

            }else if (taginfo.getTagValueType().equals(ConstCodes.TAG_VALUE_TYPE_DECIMAL)){

                taskListDecimal.add(taginfo);
            }

        }

        // 拼写表名

        String mergeTbleName = "tg_merge_profile_"+taskDate.replace("-","");

        //调用方法执行

        creatOrInsertTable(taskListString,mergeTbleName,"user_tag_value_string",taskDate);
        creatOrInsertTable(taskListLong,mergeTbleName,"user_tag_value_long",taskDate);
        creatOrInsertTable(taskListDecimal,mergeTbleName,"user_tag_value_decimal",taskDate);
        creatOrInsertTable(taskListDate,mergeTbleName,"user_tag_value_date",taskDate);



    }

    public static void creatOrInsertTable(List<Taginfo> taginfoList, String mergeTbleName,String bitmapName,String taskDate){


        if (taginfoList.size()>0) {

            List<String> tagValueList = taginfoList.stream().map(taginfo -> "(" + taginfo.getTagCode().toLowerCase() + "" + ", '" + taginfo.getTagCode().toLowerCase() + "')").collect(Collectors.toList());

            String tagvalue = StringUtils.join(tagValueList, " ,");

            // 查询 clickhouse 里面的宽表  语句拼写

            String selectSql = "select  tp.2 as tag_code, tp.1 as tag_value," +
                    " groupBitmapState(cast(uid as UInt64)) as us,\n" +
                    " '"+taskDate+"'\n" +
                    "from (\n" +
                    "     select uid,\n" +
                    "       arrayJoin([" + tagvalue + "]) as tp\n" +
                    "from " + mergeTbleName + "\n" +
                    ")\n" +
                    "group by tag_code,tag_value";

            //插入语句

            String insertSql = "insert into " + bitmapName + " " + selectSql + "";
            //System.out.println(insertSql);
            // 幂等性sql

            String deleteSql = "alter table "+bitmapName+" delete where dt = '"+taskDate+"'";


            // 执行
            MyClickhouseUtil.executeSql(deleteSql);

            MyClickhouseUtil.executeSql(insertSql);


        }

    }
}
