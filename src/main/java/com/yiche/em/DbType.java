package com.yiche.em;

/**
 * Created by weiyongxu on 2018/7/4.
 */
public enum  DbType {
    Hive("hive"),

    Presto("presto"),

    UNKNOWN("-1");

    private String val;

    private DbType(String val)
    {
        this.val = val;
    }

    public String getVal()
    {
        return val;
    }

    public static DbType getDbType(String dbType)
    {
        DbType[] dbTypes = DbType.values();
        for (DbType subDbType : dbTypes)
        {
            if (subDbType.val.equalsIgnoreCase(dbType))
            {
                return subDbType;
            }
        }
        return UNKNOWN;
    }
}
