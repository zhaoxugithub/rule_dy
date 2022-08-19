package com.serendipity.engine.utils;

import com.serendipity.engine.beans.EventParam;
import com.serendipity.engine.beans.RuleConditions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * 规则模拟器，生成RuleConditions这种规则
 * <p>
 * * 规则系统第一版
 * * 需求：
 * * 获得用户事件，计算如下规则，输出结果
 * * 规则：
 * * 触发事件：  K事件，事件属性（ p2=v1 ）
 * * 画像属性条件: tag87=v2, tag26=v1
 * * 行为次数条件： 2021-06-18 ~ 当前 , 事件 C [p6=v8,p12=v5] 做过 >= 2次
 */
public class RuleMonitor {

    public static RuleConditions getRule() {

        RuleConditions ruleConditions = new RuleConditions();
        ruleConditions.setRuleId("rule_001");
        HashMap<String, String> map = new HashMap<>();
        //map.put("p2", "v2");
        //触发事件
        ruleConditions.setTriggerEvent(new EventParam("K", map, 0, -1, -1));
        //用户画像条件
        HashMap<String, String> userMap2 = new HashMap<>();
        userMap2.put("tag65", "v2");
        userMap2.put("tag26", "v1");
        ruleConditions.setUserProfileConditions(userMap2);
        //行为事件
        HashMap<String, String> actionCountMap = new HashMap<>();
        actionCountMap.put("p6", "v8");
        actionCountMap.put("p12", "v5");
        EventParam eventParam = new EventParam("C", actionCountMap, 2, 1623945600000L, Long.MAX_VALUE);
        ruleConditions.setActionCountConditions(Arrays.asList(eventParam));
        //行为序列条件
        //ruleConditions.setActionSequenceCondition();
        return ruleConditions;
    }
}
