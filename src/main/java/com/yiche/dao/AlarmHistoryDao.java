package com.yiche.dao;

import com.yiche.bean.AlarmHistoryEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * Created by weiyongxu on 2018/7/31.
 */
@Mapper
public interface AlarmHistoryDao {
    void save(AlarmHistoryEntity alarmHistoryEntity);
}
