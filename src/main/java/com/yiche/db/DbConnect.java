package com.yiche.db;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by weiyongxu on 2018/7/4.
 */
public interface DbConnect {
    /**
     * Description: 获取数据库连接对象
     *
     * @return 数据库连接对象
     * @see
     */
    Connection getConnection() throws SQLException;

    /**
     * Description:关闭连接
     *
     * @param conn
     *            连接
     * @return 是否关闭成功
     * @see
     */
    public abstract boolean closeConnection(Connection conn);
}
