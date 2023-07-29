package com.geosot.javademo.controller;

import com.geosot.javademo.accumulo.AccumuloDataReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class PointCloudController {

    @Autowired
    private final AccumuloDataReader accumuloService;

    public PointCloudController(AccumuloDataReader accumuloService) {
        this.accumuloService = accumuloService;
    }

    @GetMapping("/queryPointCloud")
    public List<Map<String, String>> queryPointCloud() {
        return accumuloService.queryPointCloud();
    }
}

