package com.atguigu.democustomer.controller;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author MengX
 * @create 2023/2/18 11:36:43
 */

@RestController
public class CustomerController {

    @RequestMapping(value ="test" )
    public String getTest(){

        System.out.println("111111");
        return "success";
    }

    @RequestMapping("customer/{id}")
    public String getTest(@PathVariable("id") String value){

        System.out.println("2222");

        return value+"1";

    }





}
