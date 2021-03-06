package com.yiche.bussiness;

import org.springframework.cloud.netflix.feign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(value = "aurora-metadata")
public interface MetadataBussiness {

    @RequestMapping(value = "/hi", method = RequestMethod.GET)
    String getMetaDataBaseInfo(@RequestParam("name") String str);

    @RequestMapping(value = "/dblist", method = RequestMethod.GET)
    String getDBList(@RequestParam("name") String name);
}