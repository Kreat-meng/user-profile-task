package com.atguigu.democustomer.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.lang.model.element.NestingKind;

/**
 * @author MengX
 * @create 2023/2/18 11:39:28
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerBean {

    private String name;

    private int age;
}
