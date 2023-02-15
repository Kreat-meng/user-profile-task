package com.atguigu.bean;

import lombok.Data;

import java.util.Date;

/**
 * @author MengX
 * @create 2023/2/14 15:00:51
 */
@Data
public class Taginfo {

    Long id;

    String tagCode;

    String tagName;

    Long tagLevel;

    Long parentTagId;

    String tagType;

    String tagValueType;

    Long tagTaskId;

    String tagComment;

    Date createTime;

    Date updateTime;



}