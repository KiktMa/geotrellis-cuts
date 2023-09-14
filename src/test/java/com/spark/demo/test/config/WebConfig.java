package com.spark.demo.test.config;

import com.spark.demo.test.interceptor.WebInterceptorDemo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Autowired
    private WebInterceptorDemo webInterceptorDemo;
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(webInterceptorDemo).addPathPatterns("/**").excludePathPatterns("/login");
    }
}
