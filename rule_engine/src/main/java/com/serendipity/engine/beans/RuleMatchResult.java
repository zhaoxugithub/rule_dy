package com.serendipity.engine.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 规则命中的bean类型
 */
@Data
@AllArgsConstructor
public class RuleMatchResult {
    //每一条数据的id,类似人的id
    private String deviceId;
    //规则ID
    private String ruleId;
    //触发规则事件时间
    long trigEventTimestamp;
    //匹配上计算时间
    long matchTimestamp;
}
