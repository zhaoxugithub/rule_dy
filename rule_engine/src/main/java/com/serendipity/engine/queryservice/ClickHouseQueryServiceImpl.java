package com.serendipity.engine.queryservice;

import com.serendipity.engine.beans.EventParam;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/*
 * 画像属性条件: tag87=v2, tag26=v1
 * 行为次数条件： 2021-06-18 ~ 当前 , 事件 C [p6=v8,p12=v5] 做过 >= 2次
 */
@Slf4j
public class ClickHouseQueryServiceImpl implements QueryService {

    private Connection chConnection = null;

    public ClickHouseQueryServiceImpl(Connection connection) {
        this.chConnection = connection;
    }

    public long queryEventCountCondition(EventParam eventParam, String deviceId, String sql) throws SQLException {
        log.debug("sql = {},deviceId = {},startTime = {},endTime = {}", sql, deviceId, eventParam.getTimeRangeStart(), eventParam.getTimeRangeEnd());
        PreparedStatement preparedStatement = chConnection.prepareStatement(sql);
        preparedStatement.setString(1, deviceId);

        ResultSet resultSet = preparedStatement.executeQuery();
        long cnt = 0;
        while (resultSet.next()) {
            cnt = resultSet.getLong("cnt");
        }

        log.debug("cnt = {}", cnt);
        return cnt;
    }
}
