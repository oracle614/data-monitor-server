package com.yiche.service.impl;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.yiche.bean.Mail;
import com.yiche.bean.MailJsonDataEntity;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SendEmailTest {
//	public static void main(String[] args) throws Exception {
//		String remoteUrl = "http://yu.yiche.com/receiveandsendmail/sendmail";
//		MailJsonDataEntity vo = new MailJsonDataEntity();
//		vo.setFrom("bdcgjxt@yiche.com");
//		List<String> to=new ArrayList<String>();
//		to.add("litongwei@yiche.com");
//		to.add("444593771@qq.com");
//		vo.setPrimaryTo(to);
//		vo.setSubject("Test Email");
//		vo.setGroupId(20+"");
//		vo.setWeixinReceiver("9449");
//
//		Map<String,Object> map=new HashMap<String,Object>();
//		//key,value方式传递数据
//		map.put("url", "999");
//		map.put("name", "li");
//		map.put("user", "litongwei");
//
//		//数组传递方式
//		List<String> books=new ArrayList<String>();
//		books.add("java编程思想");
//		books.add("c语言开发");
//		map.put("books", books);
//
//		//传递实体类
////		User u=new User();
////		u.setAge("18");
////		u.setName("xiaomi");
////		u.setSex("男");
////		List<User> users=new ArrayList<User>();
////		users.add(u);
////		map.put("users", users);
////
////
//
//		Map<String,Object> maptest=new HashMap<String,Object>();
//		List<Mail> mails=new ArrayList<Mail>();
//		Mail m=new Mail();
//		m.setIndex("1");
//		m.setDate("20171113");
//		m.setInfo("数据少于4千万");
//		m.setSql("select");
//		m.setTable("bitauto_ods.ods_detail_lzo");
//		mails.add(m);
//
//		m.setIndex("2");
//		m.setDate("2017-11-14");
//		m.setInfo("二手车pc的pv数据为0");
//		String str="select * from (select sum(pv) as num from bitauto_ods.ods_report2016_traffic where etl_dt='2017-11-13'>";
////		String s=StringEscapeUtils.escapeHtml(str);
//		m.setSql(str);
//		m.setTable("bitauto_ods.ods_report2016_traffic");
//		mails.add(m);
//		maptest.put("mails", mails);
//		vo.setData(maptest);
//		ObjectMapper mapper = new ObjectMapper();
//		String json=mapper.writeValueAsString(vo);
//		System.out.println("json: "+json);
//		MailJsonDataEntity entity=mapper.readValue(json, MailJsonDataEntity.class);
//		System.out.println(entity);
//		String result = httpPostWithJSON(remoteUrl, json);
//		System.out.println(result);
//	}
//
//	public static String httpPostWithJSON(String url, String json)
//			throws Exception {
//
//		HttpPost httpPost = new HttpPost(url);
//		HttpClient client = new DefaultHttpClient();
//		String respContent = null;
//
//		// json方式
//		StringEntity entity = new StringEntity(json.toString(), "utf-8");// 解决中文乱码问题
//		entity.setContentEncoding("UTF-8");
//		entity.setContentType("application/json");
//		httpPost.setEntity(entity);
//		System.out.println("POST的json数据格式如下: \n" + json.toString());
//
//		HttpResponse resp = client.execute(httpPost);
//		if (resp.getStatusLine().getStatusCode() == 200) {
//			HttpEntity he = resp.getEntity();
//			respContent = EntityUtils.toString(he, "UTF-8");
//			System.out.println("发送数据成功====================");
//		}
//		return respContent;
//	}

//	public static void main(String[] args) {
//		   ExecutorService cachedThreadPool = Executors.newFixedThreadPool(3);
//		   for (int i = 0; i < 10; i++) {
//			    final int index = i;
//			   try {
//				     Thread.sleep(1000);
//				    } catch (InterruptedException e) {
//				     e.printStackTrace();
//				    }
//			   cachedThreadPool.execute(new Runnable() {
//     public void run() {
//					      System.out.println(index);
//		 System.out.println(Thread.currentThread().getName()+"运行,  ");
//					     }
//   });
//			  }
//	}

	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);

		int N = Integer.parseInt(args[0]);

		double[] ave = new double[N];
		double sum = 0.0;
		for(int i = 1; sc.hasNext(); i++) {
			sum -= ave[i % N];
			ave[i % N] = sc.nextDouble();
			sum += ave[i % N];
			if(i >= N) System.out.print(sum/N + " ");
		}
	}
}
