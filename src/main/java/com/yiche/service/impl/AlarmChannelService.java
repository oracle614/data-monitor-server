package com.yiche.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.yiche.utils.MessageUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by weiyongxu on 2018/7/30.
 */
public class AlarmChannelService {
    private static Logger logger = LoggerFactory.getLogger(AlarmChannelService.class);

    public static boolean weixinAlarm(String subject, String body, List<Integer> weixinUserList) {
        String weixinToUsers = StringUtils.join(weixinUserList, "|");
        String ret = MessageUtils.sendMessageByDefault(null, "weiyx@yiche.com", null,
                null, null, null, subject,
                body, weixinToUsers, null, true);

        logger.info("send weixin alarm result:{}, weixinUsers:{}, body:{}", ret, weixinToUsers, body);

        JSONObject jsonObject = JSONObject.parseObject(ret);
        String code = jsonObject.getString("code");
        if("0".equals(code)) {
            return true;
        }

        logger.error("send weixin alarm failed:{}, weixinUsers:{}, body:{}", ret, weixinToUsers, body);
        return false;
    }

    public static boolean mailAlarm(String subject, String body, List<String> mailToList) {
        String ret = MessageUtils.sendMessageByDefault(null, "weiyx@yiche.com", null,
                mailToList, null, null, subject,
                body, null, null, true);

        logger.info("send mail alarm result:{}, mailUsers:{}, mailbody:{}", ret, mailToList, body);

        JSONObject jsonObject = JSONObject.parseObject(ret);
        String code = jsonObject.getString("code");
        if("0".equals(code)) {
            return true;
        }

        logger.error("send mail alarm failed:{}, mailUsers:{}, mailbody:{}", ret, mailToList, body);
        return false;
    }
}
