package com.geosot.javademo.controller;

import com.geosot.javademo.entity.PointCloud;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 请求参数接收
 */
@RestController
public class RequestDemo {

    @GetMapping("/simpleParam")
    public String simpleParam(@RequestParam(name = "name") String username,Integer age){
        System.out.println(username+":"+age);
        return "OK";
    }

    @GetMapping("/simpleInstance")
    public String simpleInstance(PointCloud pointCloud){
        System.out.println(pointCloud);
        return "OK";
    }

    @GetMapping("/simpleArray")
    public String simpleArray(@RequestParam List<String> hobby){
        System.out.println(hobby);
        return "OK";
    }

    @GetMapping("/simpleTime")
    public String simpleTime(@DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")LocalDateTime updataTime){
        System.out.println(updataTime);
        return "OK";
    }

    @GetMapping("/simpleJson")
    public String simpleJson(@RequestBody PointCloud pointCloud){
        System.out.println(pointCloud);
        return "OK";
    }

    @GetMapping("/simpleVarible/{id}")
    public String simpleVarible(@PathVariable Integer id){
        System.out.println(id);
        return "OK";
    }

    @GetMapping("/simpleVarible2/{id}/{name}")
    public String simpleVarible2(@PathVariable Integer id,@PathVariable String name){
        System.out.println(id+":"+name);
        return "OK";
    }
}
