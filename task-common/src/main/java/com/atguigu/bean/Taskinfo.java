package com.atguigu.bean;

import lombok.Data;

import java.util.Date;

/**
 * @author MengX
 * @create 2023/2/14 15:00:37
 */


@Data
public class Taskinfo {

    Long id;

    String taskName;

    String taskStatus;

    String taskComment;

    String taskType;

    String execType;

    String mainClass;

    Long fileId;

    String taskArgs;

    String taskSql;

    Long taskExecLevel;

    Date createTime;

}
