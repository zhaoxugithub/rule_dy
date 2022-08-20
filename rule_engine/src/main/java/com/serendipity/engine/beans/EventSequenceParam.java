package com.serendipity.engine.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * 因为行为序列规则中可能要求具有多个不同的行为序列
 * <p>
 * 所以要封装一个行为序列
 */
@Data
@AllArgsConstructor
public class EventSequenceParam {
    private String ruleId;
    private long timeRangeStart;
    private long timeRangeEnd;
    private List<EventParam> eventSequece;
    //一个行为序列（A,B,C） 需要封装成一条sql
    private String sequenceQuerySql;
}
