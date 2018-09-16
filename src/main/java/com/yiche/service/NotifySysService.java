package com.yiche.service;

import com.yiche.bean.IndexProMail;

import java.util.List;

public interface NotifySysService {
 /*   /**
     * 报警
     * @param from
     * @param to
     * @param groupId
     * @param weixinReceiver
     * @param dateBase
     * @param tableName
     * @param content
     * @param column
     * @param error
     * @param value
     * @param valueCompare
     * @param scope
     * @param id
     * @param project
     * @param waveScope
     * @param checkDay
     */
  /*  void notifyWechat(String from,List<String> to,String groupId,String weixinReceiver,String dateBase
            ,String tableName,String content,String column,String error,String value
            ,String valueCompare,String scope,String id,String project,String waveScope,Integer checkDay,String partitionType);

    /**
     * 指数
     * @param from
     * @param to
     * @param groupId
     * @param weixinReceiver
     * @param mailList
     */
  /*  void notifyWechat(String from, List<String> to, String groupId, String weixinReceiver,List<IndexProMail> mailList);

    /**
     * sql
     * @param from
     * @param to
     * @param groupId
     * @param weixinReceiver
     * @param dateBase
     * @param tableName
     * @param content
     */
 /*   void notifyWechat(String from,List<String> to,String groupId,String weixinReceiver,String dateBase
            ,String tableName,String content);

*/

    /**
     *
     * @param alarmUniqueId
     * @param mailList
     */
    void notifyBuilder(String alarmUniqueId, List<IndexProMail> mailList);


    /**
     * 报警
     * @param alarmUniqueId
     * @param dateBase
     * @param tableName
     * @param content
     * @param column
     * @param error
     * @param value
     * @param valueCompare
     * @param scope
     * @param id
     * @param project
     * @param waveScope
     * @param checkDay
     */
    void notifyBuilder(String alarmUniqueId, String dateBase
            , String tableName, String content, String column, String error, String value
            , String valueCompare, String scope, String id, String project, String waveScope, Integer checkDay, String partitionType);

    /**
     * sql
     * @param alarmUniqueId
     * @param dateBase
     * @param tableName
     * @param content
     */
    void notifyBuilder(String alarmUniqueId,String dateBase
            , String tableName, String content);
    /**
     * sql
     * @param alarmUniqueId
     * @param subject
     * @param content
     */
    void notifyBuilder(String alarmUniqueId, String subject, String content);
}
