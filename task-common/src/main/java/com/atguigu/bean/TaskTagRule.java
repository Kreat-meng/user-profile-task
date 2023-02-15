package com.atguigu.bean;

import lombok.Data;

/**
 * @author MengX
 * @create 2023/2/14 15:01:08
 */
@Data
public class TaskTagRule {

    Long id;

    Long tagId;

    Long taskId;

    String queryValue;

    Long subTagId;

    String subTagValue;

}
