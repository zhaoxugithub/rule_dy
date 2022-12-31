package com.serendipity.engine.queryservice;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * 封装hbase的查询逻辑
 */
@Slf4j
public class HbaseQueryServiceImpl implements QueryService {

    private Connection hbaseConnection = null;

    public HbaseQueryServiceImpl(Connection connection) {
        this.hbaseConnection = connection;
    }

    /**
     * 查询hbase 中的结果和规则要求是否相同
     *
     * @param deviceId
     * @param userProfileConditions
     * @return
     * @throws IOException
     */
    public boolean queryProfileCondition(String deviceId, Map<String, String> userProfileConditions) throws IOException {
        Table table = hbaseConnection.getTable(TableName.valueOf("yinew_profile"));
        // 设置hbase的查询条件：rowKey
        Get get = new Get(deviceId.getBytes());
        // 设置要查询的family和qualifier(标签名称)
        Set<String> tags = userProfileConditions.keySet();
        for (String tag : tags) {
            // tag实际上就是列名称
            get.addColumn("f".getBytes(), tag.getBytes());
        }
        // 请求hbase查询hbase
        Result result = table.get(get);
        // 根据规则条件去查询可能查询出来的是空值，所以这里需要进行判空操作
        if (result.isEmpty()) {
            return false;
        }
        for (String tag : tags) {
            byte[] value = result.getValue("f".getBytes(), tag.getBytes());
            log.debug("hbase 查询结果为={}", new String(value));
            if (!userProfileConditions.get(tag).equals(new String(value))) {
                return false;
            }
        }
        return true;
    }
}
