package com.yiche.bussiness;


import org.springframework.cloud.netflix.feign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@FeignClient(value = "monitor-data-platform")
public interface PlatformBussiness {


        @RequestMapping(value = "/bdc-fast/yiche/column/queryAllCRuler",method = RequestMethod.GET)
        String getColumnRule();

        @RequestMapping(value ="/bdc-fast/yiche/table/queryAllTRuler",method = RequestMethod.GET)
        String getTableRule();



}
