package com.atguigu.DAO;

import com.atguigu.bean.TaskTagRule;
import com.atguigu.untils.Mysqluntils;

import java.util.List;

/**
 * @author MengX
 * @create 2023/2/14 19:57:23
 */
public class TaskTagRuleDAO {

    public static List<TaskTagRule> getTagtaskrule(String taskId){

        //过去 Tagtaskrule 对象

        String sql = "select tr.id,tr.tag_id,tr.task_id task_id,tr.query_value,tr.sub_tag_id,ta.tag_name sub_tag_value " +
                "from task_tag_rule tr " +
                "join tag_info ta " +
                "on tr.sub_tag_id = ta.id " +
                "where tr.task_id = '"+ taskId + "'";

        List<TaskTagRule> taskTagRules = Mysqluntils.queryList(sql, TaskTagRule.class, true);

        return taskTagRules;


    }
}
