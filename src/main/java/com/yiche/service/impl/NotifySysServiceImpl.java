package com.yiche.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yiche.bean.IndexProMail;
import com.yiche.bean.Mail;
import com.yiche.bean.MailJsonDataEntity;
import com.yiche.bean.SqlMail;
import com.yiche.service.NotifySysService;
import com.yiche.utils.DateFormatSafe;
import com.yiche.utils.FinalVar;
import com.yiche.utils.NoticeBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class NotifySysServiceImpl implements NotifySysService {
    private final Logger logger = LoggerFactory.getLogger(NotifySysServiceImpl.class);
    @Value("${warnning.url}")
    private String remoteUrl;


    //数据报告
    @Override
    public void notifyBuilder(String alarmUniqueId, List<IndexProMail> mailList) {

        NoticeBuilder noticeBuilder = NoticeBuilder.createNoticeSend();
        noticeBuilder.setGroupUniqueId(alarmUniqueId);
//        noticeBuilder.setWxToList("15342");
//        noticeBuilder.setEmailToList("zhaoguanchen@yiche.com");

        noticeBuilder.setEmailSubject("项目报告");
        Map<String, Object> mapDate = new HashMap<String, Object>();
        mapDate.put("mails", mailList);
        ObjectMapper mapper = new ObjectMapper();
        String content = null;

        try {
            content = mapper.writeValueAsString(mapDate);
            System.out.println(content);
            noticeBuilder.setDataContent(content);
            noticeBuilder.sendNotice();
        } catch (Exception e) {
            logger.info("notify failed", e);
        }


    }



    //warning
    @Override
    public void notifyBuilder(String alarmUniqueId, String dateBase
            , String tableName, String content, String column, String error, String value
            , String valueCompare, String scope, String id, String project, String waveScope, Integer checkDay, String partitionType) {


        Map<String, Object> mapDate = new HashMap<String, Object>();
        List<Mail> mailList = new ArrayList<Mail>();
        Mail mail = new Mail();

        mail.setInfo(content);
        mail.setId(id);
        mail.setTable(dateBase + "." + tableName + "| " + column);
        mail.setType(error);

        mail.setDate(FinalVar.MONTH.equals(partitionType) ?
                DateFormatSafe.formatMonth(DateFormatSafe.getMonth(checkDay)) :
                DateFormatSafe.formatSign(DateFormatSafe.getDay(checkDay)));
        mail.setValue(value);
        mail.setValueCompare(valueCompare);
        mail.setScope(scope);
        mail.setProject(project);
        mail.setWaveScope(waveScope);
        mailList.add(mail);

        mapDate.put("mails", mailList);

        NoticeBuilder noticeBuilder = NoticeBuilder.createNoticeSend();
        noticeBuilder.setGroupUniqueId(alarmUniqueId);
        noticeBuilder.setEmailSubject("规则告警");

        ObjectMapper mapper = new ObjectMapper();
        String datacontent = null;

        try {
            datacontent = mapper.writeValueAsString(mapDate);
            System.out.println(datacontent);
            noticeBuilder.setDataContent(datacontent);
            noticeBuilder.sendNotice();
        } catch (Exception e) {
            logger.info("notify failed", e);
        }

//        try {
//            json = mapper.writeValueAsString(vo);
//            logger.info("json:{} " + json);
//            MailJsonDataEntity entity = mapper.readValue(json, MailJsonDataEntity.class);
//            logger.info(entity.toString());
//            String result = httpPostWithJSON(remoteUrl, json);
//        } catch (Exception e) {
//            logger.info("报警失败", e);
//        }
    }


    //sql
    @Override
    public void notifyBuilder(String alarmUniqueId, String dateBase
            , String tableName, String content) {

        NoticeBuilder noticeBuilder = NoticeBuilder.createNoticeSend();
        noticeBuilder.setGroupUniqueId(alarmUniqueId);
//        noticeBuilder.setWxToList("15342");
//        noticeBuilder.setEmailToList("zhaoguanchen@yiche.com");

        noticeBuilder.setEmailSubject("项目报告");


        Map<String, Object> mapDate = new HashMap<String, Object>();
        List<SqlMail> mailList = new ArrayList<>();
        SqlMail mail = new SqlMail();

        mail.setResult(content);
        mailList.add(mail);

        mapDate.put("mails", mailList);


        ObjectMapper mapper = new ObjectMapper();
        String datacontent = null;

        try {
            datacontent = mapper.writeValueAsString(mapDate);
            System.out.println(datacontent);
            noticeBuilder.setDataContent(datacontent);
            noticeBuilder.sendNotice();
        } catch (Exception e) {
            logger.info("notify failed", e);
        }


    }
    @Override
    public void notifyBuilder(String alarmUniqueId, String subject, String content){
        NoticeBuilder noticeBuilder = NoticeBuilder.createNoticeSend();
        noticeBuilder.setGroupUniqueId(alarmUniqueId);


        noticeBuilder.setEmailSubject("规则执行失败告警");

        Map<String, Object> mapDate = new HashMap<String, Object>();
        mapDate.put("subject",subject);
        mapDate.put("content",content);

        ObjectMapper mapper = new ObjectMapper();
        String datacontent = null;

        try {
            datacontent = mapper.writeValueAsString(mapDate);
            System.out.println(datacontent);
            noticeBuilder.setDataContent(datacontent);
            noticeBuilder.sendNotice();
        } catch (Exception e) {
            logger.info("notify failed", e);
        }





    }

 /*   ///////////////////////////分割线///////////////////////////////////////

    //warning
    @Override
    public void notifyWechat(String from, List<String> to, String groupId, String weixinReceiver, String dateBase
            , String tableName, String content, String column, String error, String value
            , String valueCompare, String scope, String id, String project, String waveScope, Integer checkDay, String partitionType) {
        MailJsonDataEntity vo = new MailJsonDataEntity();
        vo.setFrom(from);
        vo.setPrimaryTo(to);
        vo.setSubject(dateBase + "." + tableName + FinalVar.WARNING);
        vo.setGroupId(groupId);
        vo.setWeixinReceiver(weixinReceiver);

        Map<String, Object> mapDate = new HashMap<String, Object>();
        List<Mail> mailList = new ArrayList<Mail>();
        Mail mail = new Mail();

        mail.setInfo(content);
        mail.setId(id);
        mail.setTable(dateBase + "." + tableName + "| " + column);
        mail.setType(error);
        // FinalVar.MONTH.equals(partitionType)? DateFormatSafe.getMonth(checkDay):DateFormatSafe.getDay(checkDay)

        mail.setDate(FinalVar.MONTH.equals(partitionType) ?
                DateFormatSafe.formatMonth(DateFormatSafe.getMonth(checkDay)) :
                DateFormatSafe.formatSign(DateFormatSafe.getDay(checkDay)));
        mail.setValue(value);
        mail.setValueCompare(valueCompare);
        mail.setScope(scope);
        mail.setProject(project);
        mail.setWaveScope(waveScope);
        mailList.add(mail);

        mapDate.put("mails", mailList);
        vo.setData(mapDate);
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(vo);
            logger.info("json:{} " + json);
            MailJsonDataEntity entity = mapper.readValue(json, MailJsonDataEntity.class);
            logger.info(entity.toString());
            String result = httpPostWithJSON(remoteUrl, json);
        } catch (Exception e) {
            logger.info("报警失败", e);
        }
    }

    //sql
    @Override
    public void notifyWechat(String from, List<String> to, String groupId, String weixinReceiver, String dateBase
            , String tableName, String content) {
        MailJsonDataEntity vo = new MailJsonDataEntity();
        vo.setFrom(from);
        vo.setPrimaryTo(to);
        vo.setSubject(dateBase + "." + tableName + FinalVar.SQL);
        vo.setGroupId(groupId);
        vo.setWeixinReceiver(weixinReceiver);

        Map<String, Object> mapDate = new HashMap<String, Object>();
        List<SqlMail> mailList = new ArrayList<>();
        SqlMail mail = new SqlMail();

        mail.setResult(content);
        mailList.add(mail);

        mapDate.put("mails", mailList);
        vo.setData(mapDate);
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(vo);
            logger.info("json:{} " + json);
            MailJsonDataEntity entity = mapper.readValue(json, MailJsonDataEntity.class);
            logger.info(entity.toString());
            String result = httpPostWithJSON(remoteUrl, json);
        } catch (Exception e) {
            logger.info("报警失败", e);
        }
    }

    //数据报告
    @Override
    public void notifyWechat(String from, List<String> to, String groupId, String weixinReceiver, List<IndexProMail> mailList) {
        MailJsonDataEntity vo = new MailJsonDataEntity();
        vo.setFrom(from);
        vo.setPrimaryTo(to);
        vo.setSubject("项目报告");
        vo.setGroupId(groupId);
        vo.setWeixinReceiver(weixinReceiver);

        Map<String, Object> mapDate = new HashMap<String, Object>();
        mapDate.put("mails", mailList);
        vo.setData(mapDate);
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(vo);
            logger.info("json: " + json);
            MailJsonDataEntity entity = mapper.readValue(json, MailJsonDataEntity.class);
            logger.info(entity.toString());
            String result = httpPostWithJSON(remoteUrl, json);
        } catch (Exception e) {
            logger.info("报警失败", e);
        }
    }

*/
    public String httpPostWithJSON(String url, String json)
            throws Exception {

        HttpPost httpPost = new HttpPost(url);
        HttpClient client = new DefaultHttpClient();
        String respContent = null;

        // json方式
        StringEntity entity = new StringEntity(json, "utf-8");// 解决中文乱码问题
        entity.setContentEncoding("UTF-8");
        entity.setContentType("application/json");
        httpPost.setEntity(entity);
        logger.info("POST的json数据格式如下: \n" + json);

        HttpResponse resp = client.execute(httpPost);
        if (resp.getStatusLine().getStatusCode() == 200) {
            HttpEntity he = resp.getEntity();
            respContent = EntityUtils.toString(he, "UTF-8");
            logger.info("发送数据成功====================");
        }
        return respContent;
    }

}
