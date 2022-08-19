package com.serendipity.engine.beans;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

/**
 * 规则条件中，最原子的一个封装，封装“1个”事件条件
 */
@Data
@AllArgsConstructor
public class EventParam {
    //事件id
    private String eventId;
    //事件属性
    private Map<String, String> eventProperties;
    //事件触发次数阈值
    private int countThreshold;

    //表示的是事件触发的开始时间
    private long timeRangeStart;

    //表示的是事件触发的结束时间
    private long timeRangeEnd;

    //一个事件产生了就意味着一条查询ck的sql已经产生，也是属于触发规则中的一个
    private String querySql;
}
