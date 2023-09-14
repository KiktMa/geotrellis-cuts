package com.spark.demo.test.exception;

import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 全局异常处理器
 */
@RestControllerAdvice // 添加这个注解即添加了一个全局异常处理器
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class) // 表示接收所有的异常
    public void ex(Exception exception){
        exception.printStackTrace();
        // return R.error();
    }
}
