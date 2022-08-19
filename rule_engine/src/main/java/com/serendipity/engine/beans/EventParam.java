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
}
