package com.yiche;

import com.yiche.bean.ColumnRuleBean;
import com.yiche.db.ConnectFactory;
import com.yiche.em.DbType;
import com.yiche.service.ColumnRuleService;
import com.yiche.service.ExcutingRuleService;
import com.yiche.service.NotifySysService;
import com.yiche.service.SyncRuleExecTimeService;
import com.yiche.utils.FinalVar;
import com.yiche.utils.NoticeBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;


@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("dev")
public class QcApplicationTests {

    @Autowired
    ExcutingRuleService excutingRuleService;

    @Autowired
    SyncRuleExecTimeService syncRuleExecTimeService;

    @Autowired
    NotifySysService  notifySysService;

    @Autowired
    ColumnRuleService columnRuleService;
    @Autowired
    private ConnectFactory connectFactory;



    @Test
    public void tableRuleRunTest() {
        ;
        excutingRuleService.runTableRule();
    }

    @Test
    public void columnRuleRunTest() {
        excutingRuleService.runColumnRule();
    }

    @Test
    public void syncRuleExecTimeTest() {
        syncRuleExecTimeService.checkRuleExecTime();
    }

    @Test
    public void runDistributorProTest() {
        String reciever = "zhaoguanchen@yiche.com|15342,weiyx@yiche.com|14741";
        excutingRuleService.runDistributorPro(FinalVar.DAY);

    }

    @Test
    public void notifyBuilder() {
        NoticeBuilder noticeBuilder = NoticeBuilder.createNoticeSend();
        noticeBuilder.setGroupUniqueId("1");
        noticeBuilder.setDataContent("2");
        try {
            boolean res = noticeBuilder.sendNotice();
            System.out.println("啊啊啊啊" + res);
        } catch (Exception e) {
            System.out.println("cuole");
        }

    }





    @Test
    public void exeTest() {
    Vector<ColumnRuleBean> vector=columnRuleService.getColumnRule();
        System.out.println(vector);
        columnRuleService.columnRuleRun();

    }



    @Test
    public void prestoTest() throws Exception{

        Statement stmt = null;
        Connection con = null;
        try {
            con = connectFactory.createConnect(DbType.Presto);
            stmt = con.createStatement();

            StringBuilder builder = new StringBuilder();
            builder.append("select * from ttzgc.t1");

            List<String> partitionList = new ArrayList<String>();
            ResultSet res = null;
            try {
                res = stmt.executeQuery("show partitions from testzgc.t2");

                while (res.next()) {
                    System.out.println(res);
                    partitionList.add(res.getMetaData().getColumnName(1) + "=" + res.getString(1));
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new Exception(e.getMessage());
            } finally {
                closeQuery(res);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeAfterQuery(stmt, con);
        }


    }

    public void closeQuery(ResultSet res) {
        if (res != null) {
            try {
                res.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                res = null;
            }
        }
    }

    public void closeAfterQuery(Statement stmt, Connection con) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
            }
        }
        if (con != null) {
            try {
                con.close();
                FinalVar.dbConnCount--;
            //    logger.info("hiveConnect关闭");
            } catch (SQLException e) {
                FinalVar.dbConnCount++;
          //      logger.error("hiveConnect关闭失败",e);
            }
        }
    }
}
