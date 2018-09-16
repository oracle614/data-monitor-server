package com.yiche.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;


/**
 * 邮件发送工具类
 *
 */
public class MessageUtils
{
    private final static String DEFAULT_GROUP_ID = "0";

//    public static void main(String[] args)
//        throws Exception
//    {
//        List<String> to = new ArrayList<String>();
//        String json = "{\"mail\":\"kkkkkkkkkkk\"}";
//        to.add("wangbolin@yiche.com");
//        to.add("weiyx@yiche.com");
//        List<String> cc = new ArrayList<String>();
//        cc.add("chengchao@yiche.com");
//        List<String> bcc = new ArrayList<String>();
//        bcc.add("litongwei@yiche.com");
//        List<String> hdfsPath = new ArrayList<String>();
//        // hdfsPath.add("/bitauto/presto_result/liuming1/bed4314d66fb4ffb8612ab67f71cc11allllllk");
//        hdfsPath.add("/bitauto/presto_result/liuming1/bed4314d66fb4ffb8612ab67f71cc11a");
//        // hdfsPath.add("/bitauto/presto_result/liuming1/1b4aafed89e04bffa675bc4baccaffce");
//        // String
//        // s=sendMessage("test","bdcgjxt@yiche.com",to,cc,bcc,"Test Email",json,25+"",null,hdfsPath)
//        // ;
//        String s = sendMessageByDefault("test", "bdcgjxt@yiche.com", "大数据中心", to, cc, bcc,
//            "Test Email", " sss ", null, hdfsPath, Boolean.TRUE);
//        // System.out.println(s);
//
//    }

    /**
     * 默认发送邮件接口，该接口只适用key-value形式的传值，只能传递一个内容，一个值，采用默认模板${mail}
     *
     * @param env
     *            是否是测试环境
     * @param from
     *            邮件发送人
     * @param primaryTo
     *            邮件收件人
     * @param carbonCopy
     *            邮件抄送人
     * @param blindCarbonCopy
     *            密送收件人
     * @param subject
     *            邮件主题
     * @param data
     *            邮件内容，只适合一个string类型的值。不适合复杂类型的传值
     * @param weixinReceiver
     *            微信接受者，格式如：员工编号1|员工编号2|.......,如果是一个人，只传递一个员工编号：员工编号1
     * @param hdfsPath
     *            HDFS路径 可以传入多个目录。
     * @param immediateExecute
     *            是否立即执行,传入 Boolean.TRUE表示立即执行，传入Boolean.FALSE表示不是立即执行，异步发送
     * @return 返回 服务端的响应码
     */

    public static String sendMessageByDefault(String env, String from, String aliasName,
                                              List<String> primaryTo, List<String> carbonCopy,
                                              List<String> blindCarbonCopy, String subject,
                                              String data, String weixinReceiver,
                                              List<String> hdfsPath, Boolean immediateExecute)
    {
        MailJsonDataEntity entity = new MailJsonDataEntity();
        entity.setCarbonCopy(carbonCopy);
        if (!isBlank(data))
        {
            String json = "{\"mail\":\"" + data + "\"}";
            entity.setJson(json);
        }
        else
        {
            String json = "{\"mail\":null}";
            entity.setJson(json);
        }
        entity.setAliasName(aliasName);
        entity.setFrom(from);
        entity.setGroupId(DEFAULT_GROUP_ID);
        entity.setPrimaryTo(primaryTo);
        entity.setHdfsPath(hdfsPath);
        entity.setSubject(subject);
        entity.setWeixinReceiver(weixinReceiver);
        entity.setBlindCarbonCopy(blindCarbonCopy);
        entity.setImmediateExecute(immediateExecute);
        String json = beanToJson(entity);
        String urlPath = "http://192.168.15.46:9820/receiveandsendmail/sendmail";
        if ("test".equals(env))
        {
            urlPath = "http://192.168.15.46:9820/receiveandsendmail/sendmail";
//            urlPath = "http://localhost:9820/receiveandsendmail/sendmail";
        }
        else
        {
            urlPath = "http://yu.yiche.com/receiveandsendmail/sendmail";
        }
        String result = doJsonPost(urlPath, json);
        return result;
    }

    /**
     * 发送邮件接口，该接口适用于传递复杂结构的对象数据，自己要在数据通道平台配置模板，
     * 定义自己的模板，定义填充数据的标签，然后传递json数据格式发送邮件内容。
     *
     * @param env
     *            是否是测试环境 ，传入test字符串表示发送到测试服务器上。
     * @param from
     *            邮件发送人
     * @param primaryTo
     *            邮件收件人
     * @param carbonCopy
     *            邮件抄送人
     * @param blindCarbonCopy
     *            密送收件人
     * @param subject
     *            邮件主题
     * @param data
     *            邮件内容，需要用户自己拼接或者使用json库序列化自己的对象成json字符串，并且和自定义的模板匹配，方可发送成功
     * @param weixinReceiver
     *            微信接受者，格式如：员工编号1|员工编号2|.......,如果是一个人，只传递一个员工编号：员工编号1
     * @param hdfsPath
     *            HDFS路径 可以传入多个目录。
     * @param immediateExecute
     *            是否立即执行,传入 Boolean.TRUE表示立即执行，传入Boolean.FALSE表示不是立即执行，异步发送
     * @return 返回 服务端的响应码
     */
    public static String sendMessage(String env, String from, List<String> primaryTo,
                                     List<String> carbonCopy, List<String> blindCarbonCopy,
                                     String subject, String data, String groupId,
                                     String weixinReceiver, List<String> hdfsPath,
                                     Boolean immediateExecute)
    {
        MailJsonDataEntity entity = new MailJsonDataEntity();
        entity.setCarbonCopy(carbonCopy);
        entity.setJson(data);
        entity.setFrom(from);
        entity.setGroupId(groupId);
        entity.setPrimaryTo(primaryTo);
        entity.setHdfsPath(hdfsPath);
        entity.setSubject(subject);
        entity.setWeixinReceiver(weixinReceiver);
        entity.setBlindCarbonCopy(blindCarbonCopy);
        entity.setImmediateExecute(immediateExecute);
        String json = beanToJson(entity);
        String urlPath = "http://192.168.15.46:9820/receiveandsendmail/sendmail";
        if ("test".equals(env))
        {
            urlPath = "http://192.168.15.46:9820/receiveandsendmail/sendmail";
            // urlPath = "http://localhost:9820/receiveandsendmail/sendmail";
        }
        else
        {
            urlPath = "http://yu.yiche.com/receiveandsendmail/sendmail";
        }
        String result = doJsonPost(urlPath, json);
        return result;
    }

    /**
     * Description: 将一个实体对象转化为JSON字符串
     *
     * @param o
     *            需要转化的对象
     * @return
     * @see
     */

    private static String beanToJson(MailJsonDataEntity o)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"from\":");
        if (o.getFrom() != null && !isBlank(o.getFrom()))
        {
            sb.append("\"" + o.getFrom() + "\",");
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"aliasName\":");
        if (o.getAliasName() != null && !isBlank(o.getAliasName()))
        {
            sb.append("\"" + o.getAliasName() + "\",");
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"primaryTo\":");
        if (!isListBlank(o.getPrimaryTo()))
        {
            sb.append("[");
            for (int i = 0; i < o.getPrimaryTo().size(); i++ )
            {
                sb.append("\"" + o.getPrimaryTo().get(i) + "\"");
                if (i != o.getPrimaryTo().size() - 1)
                {
                    sb.append(",");
                }
                else
                {
                    sb.append("],");
                }
            }
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"carbonCopy\":");
        if (o.getCarbonCopy() != null && o.getCarbonCopy().size() != 0)
        {
            sb.append("[");
            for (int i = 0; i < o.getCarbonCopy().size(); i++ )
            {
                sb.append("\"" + o.getCarbonCopy().get(i) + "\"");
                if (i != o.getCarbonCopy().size() - 1)
                {
                    sb.append(",");
                }
                else
                {
                    sb.append("],");
                }
            }
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"blindCarbonCopy\":");
        if (!isListBlank(o.getBlindCarbonCopy()))
        {
            sb.append("[");
            for (int i = 0; i < o.getBlindCarbonCopy().size(); i++ )
            {
                sb.append("\"" + o.getBlindCarbonCopy().get(i) + "\"");
                if (i != o.getBlindCarbonCopy().size() - 1)
                {
                    sb.append(",");
                }
                else
                {
                    sb.append("],");
                }
            }
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"subject\":");
        if (o.getSubject() != null && !isBlank(o.getSubject()))
        {
            sb.append("\"" + o.getSubject() + "\",");
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"data\":");
        if (o.getJson() != null && !isBlank(o.getJson()))
        {
            sb.append(o.getJson() + ",");
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"userId\":");
        if (o.getUserId() != null && !isBlank(o.getUserId()))
        {
            sb.append("\"" + o.getUserId() + "\",");
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"groupId\":");
        if (o.getGroupId() != null && !isBlank(o.getGroupId()))
        {
            sb.append("\"" + o.getGroupId() + "\",");
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"weixinReceiver\":");
        if (o.getWeixinReceiver() != null && !isBlank(o.getWeixinReceiver()))
        {
            sb.append("\"" + o.getWeixinReceiver() + "\",");
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"immediateExecute\":");
        if (o.getImmediateExecute() != null && !isBlank(o.getImmediateExecute().toString()))
        {
            sb.append("\"" + o.getImmediateExecute() + "\",");
        }
        else
        {
            sb.append("null,");
        }

        sb.append("\"hdfsPath\":");
        if (o.getHdfsPath() != null && o.getHdfsPath().size() != 0)
        {
            sb.append("[");
            for (int i = 0; i < o.getHdfsPath().size(); i++ )
            {
                sb.append("\"" + o.getHdfsPath().get(i) + "\"");
                if (i != o.getHdfsPath().size() - 1)
                {
                    sb.append(",");
                }
                else
                {
                    sb.append("]}");
                }
            }
            // sb.append("\""+o.getHdfsPath()+"\",");
        }
        else
        {
            sb.append("null}");
        }
        return sb.toString();

    }

    /**
     * 邮件推送,将构造好的内容发送到邮件服务器
     * urlPath 推送服务器的地址
     * Json 发送json字符串
     */
    private static String doJsonPost(String urlPath, String Json)
    {
        String result = "";
        BufferedReader reader = null;
        try
        {
            URL url = new URL(urlPath);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            // conn.setRequestProperty("Connection", "Keep-Alive");
            conn.setRequestProperty("Charset", "UTF-8");
            // 设置文件类型:防止乱码
            conn.setRequestProperty("Content-Type",
                    "application/json; charset=UTF-8");
            // 设置接收类型否则返回415错误
            // conn.setRequestProperty("accept","*/*")此处为暴力方法设置接受所有类型，以此来防范返回415;
            conn.setRequestProperty("accept", "application/json");
            // 往服务器里面发送数据
            if (Json != null && !isBlank(Json))
            {
                byte[] writebytes = Json.getBytes();
                // 设置文件长度
                conn.setRequestProperty("Content-Length",
                        String.valueOf(writebytes.length));
                OutputStream outwritestream = conn.getOutputStream();
                outwritestream.write(Json.getBytes());
                outwritestream.flush();
                outwritestream.close();
                // System.out.println("doJsonPost: conn" + conn.getResponseCode());
            }
            if (conn.getResponseCode() == 200)
            {
                reader = new BufferedReader(new InputStreamReader(
                        conn.getInputStream()));
                result = reader.readLine();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (reader != null)
            {
                try
                {
                    reader.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    /**
     * Description: 邮件内容是否有空格
     *
     * @param s
     *            需要检查的内容
     * @return
     * @see
     */
    private static boolean isBlank(final CharSequence s)
    {
        if (s == null)
        {
            return true;
        }
        for (int i = 0; i < s.length(); i++ )
        {
            if (!Character.isWhitespace(s.charAt(i)))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Description: 邮件内容List类型是否有空格或者为空
     *
     * @param s
     *            需要检查的内容
     * @return
     * @see
     */
    private static boolean isListBlank(List<String> s)
    {
        if (s == null || s.size() == 0)
        {
            return true;
        }
        else
        {
            return false;
        }

    }

}


/**
 * 邮件包装对象,此对象需要支持序列化
 *
 * @version 2018年2月8日
 * @see MailJsonDataEntity
 * @since
 */
class MailJsonDataEntity implements Serializable
{

    /**
     * 接收post的josn数据实体
     */
    private static final long serialVersionUID = -8718900510596587549L;

    /**
     * 邮箱发送者
     */
    private String from;

    /**
     * 邮箱发送者别名
     */
    private String aliasName;

    /**
     * 邮箱接收者
     */
    private List<String> primaryTo;

    /**
     * 邮箱抄送者
     */
    private List<String> carbonCopy;

    /**
     * 邮箱密送者
     */
    private List<String> blindCarbonCopy;

    /**
     * 邮箱主题
     */
    private String subject;

    /**
     * 邮箱内容，是一个json数据格式的字符串
     */
    private String json;

    /**
     * 用户ID留待以后用
     */
    private String userId;

    /**
     * 邮件分组Id
     */
    private String groupId;

    /**
     * 微信接收者
     */
    private String weixinReceiver;

    /**
     * HDFS目录列表
     */
    private List<String> hdfsPath;

    /**
     * 是否立即执行，true表示立即执行，false表示不立即执行
     */
    private Boolean immediateExecute;

    public Boolean getImmediateExecute()
    {
        return immediateExecute;
    }

    public void setImmediateExecute(Boolean immediateExecute)
    {
        this.immediateExecute = immediateExecute;
    }

    public List<String> getHdfsPath()
    {
        return hdfsPath;
    }

    public void setHdfsPath(List<String> hdfsPath)
    {
        this.hdfsPath = hdfsPath;
    }

    public String getWeixinReceiver()
    {
        return weixinReceiver;
    }

    public void setWeixinReceiver(String weixinReceiver)
    {
        this.weixinReceiver = weixinReceiver;
    }

    public String getGroupId()
    {
        return groupId;
    }

    public void setGroupId(String groupId)
    {
        this.groupId = groupId;
    }

    public String getUserId()
    {
        return userId;
    }

    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    public String getFrom()
    {
        return from;
    }

    public void setFrom(String from)
    {
        this.from = from;
    }

    public List<String> getPrimaryTo()
    {
        return primaryTo;
    }

    public void setPrimaryTo(List<String> primaryTo)
    {
        this.primaryTo = primaryTo;
    }

    public List<String> getCarbonCopy()
    {
        return carbonCopy;
    }

    public void setCarbonCopy(List<String> carbonCopy)
    {
        this.carbonCopy = carbonCopy;
    }

    public String getSubject()
    {
        return subject;
    }

    public void setSubject(String subject)
    {
        this.subject = subject;
    }

    public List<String> getBlindCarbonCopy()
    {
        return blindCarbonCopy;
    }

    public void setBlindCarbonCopy(List<String> blindCarbonCopy)
    {
        this.blindCarbonCopy = blindCarbonCopy;
    }

    public String getJson()
    {
        return json;
    }

    public void setJson(String json)
    {
        this.json = json;
    }

    public String getAliasName()
    {
        return aliasName;
    }

    public void setAliasName(String aliasName)
    {
        this.aliasName = aliasName;
    }

    public String toString()
    {
        return "邮件发件人   :" + this.getFrom() + "\n" + "邮件收件人   :" + this.getPrimaryTo()
                + "\n" + "邮件抄送人   :" + this.getCarbonCopy()
                + "\n" + "邮件主题  :" + this.getSubject()
                + "\n" + "模板数据  :" + this.getJson()
                + "\n" + "用户ID  :" + this.getUserId()
                + "\n" + "组ID :" + this.getGroupId()
                + "\n" + "微信接收人  :" + this.getWeixinReceiver()
                + "\n" + "HDFS路径  :" + this.getHdfsPath();
    }
}
