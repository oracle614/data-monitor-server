package com.yiche.service.impl;

import com.yiche.bean.ColumnRuleBean;
import com.yiche.dao.ColumnRuleBeanMapper;
import com.yiche.service.ColumnRuleService;
import com.yiche.service.RuleRunService;
import com.yiche.utils.DateFormatSafe;
import com.yiche.utils.FinalVar;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@Service
public class RuleRunServiceImpl implements RuleRunService {
    private final Logger logger = LoggerFactory.getLogger(ExcutingRuleServiceImpl.class);

    @Autowired
    ColumnRuleBeanMapper columnRuleBeanMapper;
    @Autowired
    ColumnRuleService columnRuleService;
    @Override
    public int insert(ColumnRuleBean columnRuleBean) {
        return columnRuleBeanMapper.insert(columnRuleBean);
    }

    @Override
    public String getNewestPartition(String dataBase, String tableName, Statement stmt) throws Exception{
        String partitions = getPartitions(dataBase, tableName, stmt);

        if(StringUtils.isEmpty(partitions)){
            return "";
        }
        return  partitions;
    }

    public void closeQuery(ResultSet res) {
        if(res != null) {
            try {
                res.close();
            } catch (SQLException e) {
                logger.error("close resultset exception, {}", e);
                e.printStackTrace();
            } finally {
                res = null;
            }
        }
    }

    @Override
    public String getPartitions(String dataBase, String tableName, Statement stmt) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("show partitions from ");
        builder.append(dataBase);
        builder.append(".");
        builder.append(tableName);
        String partitions = null;
        List<String> partitionsList= new ArrayList();
        ResultSet res = null;
        try {
            res = stmt.executeQuery(builder.toString());

            while (res.next()) {
                partitionsList.add(res.getMetaData().getColumnName(1) + "=" + res.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        } finally {
            closeQuery(res);
        }

        if(partitionsList.isEmpty()) {
            return "";
        }
        Collections.sort(partitionsList);
        return partitionsList.get(partitionsList.size()-1);
    }

    @Override
    public boolean isResultData(String dataBase, String tableName, String partitions, Statement stmt) {
        StringBuilder builder = new StringBuilder();
        //      String sql = "select * from bitauto_bdc_persona_all.index_model_user_property_month  limit 1";
        String partitionArr[] = partitions.split("=");
        builder.append("select * from ");
        builder.append(dataBase);
        builder.append(".");
        builder.append(tableName);
        builder.append(" where ");
        builder.append(tableName);
        builder.append(".");
        builder.append(partitionArr[0]);
        builder.append(" = ");
        String  date = DateFormatSafe.dateFormat(partitionArr[1],new Date());
        builder.append("'"+date+"'");
        builder.append(" limit 1 ");
        ResultSet res = null;
        try {
            logger.info(builder.toString());
            res = stmt.executeQuery(builder.toString());
            return res.next();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeQuery(res);
        }
        return false;
    }

    @Override
    public String getHDFSPath(String dataBase, String tableName, Statement stmt) {
        StringBuilder builder = new StringBuilder();
        builder.append("show create table ");
        builder.append(dataBase);
        builder.append(".");
        builder.append(tableName);
        String path= null;
        ResultSet res = null;
        try {
            res = stmt.executeQuery(builder.toString());
            builder.delete(0, builder.length());
            while (res.next()) {
                builder.append(res.getString(1));
            }
            if (builder.length() != 0) {
                String result = builder.toString();
                int locationIndex = result.indexOf("LOCATION");
                int tblpropertiesIndex = result.indexOf("TBLPROPERTIES");
                path= result.substring(locationIndex + 10, tblpropertiesIndex - 1);
              return  path;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeQuery(res);
        }
        return path;
    }

    @Override
    public boolean isFileExist(String path) {
        File file = new File("D:\\jz_activity_exchange_voucher-INSERT.txt");
        return  file.isFile() && file.exists();
}

    /**
     * 获取count总数
     * @param dataBase
     * @param tableName
     * @param column
     * @param stmt
     * @param partitions
     * @param date
     * @param where
     * @param partitionType
     * @return
     */
    @Override
    public Integer getCount(String dataBase, String tableName, String column, Statement stmt,String partitions,Date date,String where,String partitionType) throws Exception{
        String partitionArr[] = partitions.split("=");
        StringBuilder builder = new StringBuilder();
        builder.append("select ");
        builder.append("count(");
        builder.append(column==null?"*":column);
        builder.append(") from ");
        builder.append(dataBase);
        builder.append(".");
        builder.append(tableName);
        builder.append(" where ");
        builder.append(partitionArr[0]);
        builder.append(" = ");
        String dateStr=null;
        if(FinalVar.MONTH.equals(partitionType)) {
            dateStr=DateFormatSafe.dateFormatMonth(date);
        }else{
            dateStr = DateFormatSafe.dateFormat(partitionArr[1], date);
        }
        builder.append("'"+dateStr+"'");
        if(!StringUtils.isEmpty(where)){
            builder.append(" And ");
            builder.append(where);
        }
        Integer count=null;
        ResultSet res = null;
        try {
            logger.info(builder.toString());
            res = stmt.executeQuery(builder.toString());
            builder.delete(0, builder.length());
            while (res.next()) {
            count =res.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("exception count sql:{}", builder.toString());
            e.printStackTrace();
            throw new Exception(e.getMessage());
        } finally {
            closeQuery(res);
        }
        return  count;
    }

    public String getColumnType(String dataBase, String tableName, String column, Statement stmt){
        StringBuilder builder = new StringBuilder();
        builder.append(" describe ");
        builder.append(dataBase);
        builder.append(".");
        builder.append(tableName);
        String type=null;
        ResultSet res = null;
        try {
            logger.info(builder.toString());
            res = stmt.executeQuery(builder.toString());
            while (res.next()) {
                if(column.equals(res.getString(1))){
                    type=res.getString(2);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeQuery(res);
        }
        return  type;
    }

    @Override
    public Integer selectLengthOrNum(String dataBase, String tableName,Statement stmt,String select,String partitions,String where,Date date,String partitionType){

        //    select ${select,jdbcType=VARCHAR}
       // from day_date_db
        //where
       // database_name=#{databaseName,jdbcType=VARCHAR}
        //AND
       // table_name=#{tableName,jdbcType=VARCHAR}
        String partitionArr[] = partitions.split("=");
        StringBuilder builder = new StringBuilder();
        builder.append("select ");
        builder.append(select);
        builder.append(" from ");
        builder.append(dataBase);
        builder.append(".");
        builder.append(tableName);
        builder.append(" where ");
        builder.append(partitionArr[0]);
        builder.append( " = ");
        String dateStr = "";
        if(FinalVar.MONTH.equals(partitionType)) {
            dateStr=DateFormatSafe.dateFormatMonth(date);
        }else{
            dateStr = DateFormatSafe.dateFormat(partitionArr[1], date);
        }
        builder.append("'"+dateStr+"'");
        if(!StringUtils.isEmpty(where)){
            builder.append(" And ");
            builder.append(where);
        }
        Integer count=0;
        ResultSet res = null;
        try {
            logger.info(builder.toString());
            res = stmt.executeQuery(builder.toString());
            while (res.next()) {
                count =res.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("exception sum sql:{}", builder.toString());
            e.printStackTrace();
        } finally {
            closeQuery(res);
        }
        return  count;
    }

    @Override
    public Integer selectRepeatNum(String dataBase, String tableName,Statement stmt,String column,String partitions,String where,Date date,String partitionType){

        String partitionArr[] = partitions.split("=");
        StringBuilder builder = new StringBuilder();
        builder.append(" select count(*)  from (select count(");
        builder.append(column);
        builder.append(") from ");
        builder.append(dataBase);
        builder.append(".");
        builder.append(tableName);
        builder.append(" where ");
        builder.append(partitionArr[0]);
        builder.append("=");
        String dateStr = "";
        if(FinalVar.MONTH.equals(partitionType)) {
            dateStr=DateFormatSafe.dateFormatMonth(date);
        }else{
            dateStr = DateFormatSafe.dateFormat(partitionArr[1], date);
        }
        builder.append("'"+dateStr+"'");
        builder.append(" group by ");
        builder.append(column);
        builder.append(" having count(");
        builder.append(column);
        builder.append(") >1 ) a");
        if(!StringUtils.isEmpty(where)){
            builder.append(" And ");
            builder.append(where);
        }
        Integer count=null;
        ResultSet res = null;
        try {
            logger.info(builder.toString());
            res = stmt.executeQuery(builder.toString());
            while (res.next()) {
                count =res.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeQuery(res);
        }
        return  count;
    }

    public Integer selectLike(String dataBase, String tableName,Statement stmt,String column,String partitions,String where,Date date,String partitionType){
        String partitionArr[] = partitions.split("=");
        StringBuilder builder = new StringBuilder();
        builder.append(" select count(*)  from ");
        builder.append(dataBase);
        builder.append(".");
        builder.append(tableName);
        builder.append(" where ");
        builder.append(column);
        builder.append(" like '%%[^0-9,^a-z,^A-Z,^吖-座]%%' and ");
        builder.append(partitionArr[0]);
        builder.append("=");
        String dateStr = "";
        if(FinalVar.MONTH.equals(partitionType)) {
            dateStr=DateFormatSafe.dateFormatMonth(date);
        }else{
            dateStr = DateFormatSafe.dateFormat(partitionArr[1], date);
        }
        builder.append("'"+dateStr+"'");

        if(!StringUtils.isEmpty(where)){
            builder.append(" And ");
            builder.append("pv<10 group by city");
        }
        Integer count=null;
        ResultSet res = null;
        try {
            logger.info(builder.toString());
            res = stmt.executeQuery(builder.toString());
            while (res.next()) {
                count =res.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeQuery(res);
        }
        return  count;
    }

    @Override
    public Integer selectSevenDayDate(String dataBase, String tableName,Statement stmt,String partitions
            ,String where,String fromDate,String toDate){
        String partitionArr[] = partitions.split("=");
        StringBuilder builder = new StringBuilder();
        builder.append("  select ");
        builder.append(partitionArr[0]);
        builder.append(", count(*) from  ");
        builder.append(dataBase);
        builder.append(".");
        builder.append(tableName);
        builder.append(" where ");
        builder.append(partitionArr[0]);
        builder.append(">= '");
        builder.append(fromDate);
        builder.append("' and ");
        builder.append(partitionArr[0]);
        builder.append("<= '");
        builder.append(toDate);
        builder.append(" ' group by ");
        builder.append(partitionArr[0]);
        if(!StringUtils.isEmpty(where)){
            builder.append(" And ");
            builder.append(where);
        }
        Map<String,String>  map = new HashMap<>();
        Integer  sum=0;
        ResultSet res = null;
        try {
            logger.info(builder.toString());
            res = stmt.executeQuery(builder.toString());
            while (res.next()) {
              sum= sum+res.getInt(2);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeQuery(res);
        }
        Integer value = sum/7;
        return  value;
    }
@Override
    public String  getSql( String column, Statement stmt,String partitions,Date date,String sql) {
        String partitionArr[] = partitions.split("=");
        StringBuilder builder = new StringBuilder();

        builder.append(sql);
        String dateStr = DateFormatSafe.dateFormat(partitionArr[1],date);
        if(sql.contains("where")){
            builder.append(partitionArr[0]);
            builder.append(" = ");
            builder.append("'"+dateStr+"'");
        }else{
            builder.append(" where ");
            builder.append(partitionArr[0]);
            builder.append(" = ");
            builder.append("'"+dateStr+"'");
        }
       String columnArr[]= column.split(",");
//        Map<Integer,List<String>>  map= new LinkedHashMap<>();
//        for(int i =0;i<columnArr.length;i++){
//            List<String>  list = new ArrayList<>();
//            list.add(columnArr[i]);
//            map.put(i,list);
//        }
        ResultSet res = null;
        try {
            logger.info(builder.toString());
            res = stmt.executeQuery(builder.toString());
            builder.delete(0, builder.length());
            while (res.next()) {
                builder.append("<tr>");
                for(int i=0;i<columnArr.length;i++){
                    builder.append(" <td>");

//                    List<String> list=map.get(i);
//                    list.add(res.getString(i+1));
//                    map.put(i,list);
                    builder.append(res.getString(i+1));
                    builder.append(" <td>");
                }
                builder.append("</tr>");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeQuery(res);
        }
        return  builder.toString();
    }
}