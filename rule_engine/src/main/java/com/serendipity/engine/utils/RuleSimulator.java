package com.serendipity.engine.utils;

import com.serendipity.engine.beans.EventParam;
import com.serendipity.engine.beans.EventSequenceParam;
import com.serendipity.engine.beans.RuleConditions;
import org.jcodings.util.ArrayReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

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
public class RuleSimulator {

    public static RuleConditions getRule() {
        RuleConditions ruleConditions = new RuleConditions();
        ruleConditions.setRuleId("rule_001");

        HashMap<String, String> map = new HashMap<>();
        //map.put("p2", "v2");
        //触发事件
        ruleConditions.setTriggerEvent(new EventParam("K", map, 0, -1, -1, null));
        //用户画像条件
        HashMap<String, String> userMap2 = new HashMap<>();
        //userMap2.put("tag65", "v2");
        //userMap2.put("tag26", "v1");
        ruleConditions.setUserProfileConditions(userMap2);
        //行为事件
        HashMap<String, String> actionCountMap = new HashMap<>();

        String eventId = "A";
        int countThreshold = 1;

        long timeRangeStart = 1623945600000L;
        long timeRangeEnd = Long.MAX_VALUE;

        String sql = "select count(1) as cnt\n" +
                "from yinew_detail\n" +
//                "where eventId = '" + eventId + "'\n and" +
                " where  deviceId = ? \n" +
                "  and timeStamp between " + timeRangeStart + " and " + timeRangeEnd + " ";
//        actionCountMap.put("p6", "v2");
//        actionCountMap.put("p7", "v2");
        EventParam eventParam = new EventParam(eventId, actionCountMap, countThreshold, timeRangeStart, timeRangeEnd, sql);
        ruleConditions.setActionCountConditions(Arrays.asList(eventParam));
        //行为序列条件
        //表示先做什么，然后做什么，紧接着做什么
        String eventId1 = "K";
        HashMap<String, String> m1 = new HashMap<>();
        m1.put("p5", "v2");
        EventParam eventParam1 = new EventParam(eventId1, m1, 1, timeRangeStart, timeRangeEnd, "");

        String eventId2 = "X";
        HashMap<String, String> m2 = new HashMap<>();
        m2.put("p3", "v2");
        EventParam eventParam2 = new EventParam(eventId2, m2, 1, timeRangeStart, timeRangeEnd, "");

//        String eventId3 = "C";
//        //map里面封装的是条件属性
//        HashMap<String, String> m3 = new HashMap<>();
//        m2.put("p3", "v2");
//        EventParam eventParam3 = new EventParam(eventId3, m3, 1, timeRangeStart, timeRangeEnd, "");

        String sequenceQuerySQL = "select deviceId,\n" +
                "       sequenceMatch('.*(?1).*(?2).*')(\n" +
                "                     toDateTime(`timeStamp`),\n" +
                "                     eventId = 'K',\n" +
                "                     eventId = 'X'\n" +
                "\n" +
                "           ) as isMatch2,\n" +
                "       --只匹配一种\n" +
                "       sequenceMatch('.*(?1).*')(\n" +
                "                     toDateTime(`timeStamp`),\n" +
                "                     eventId = 'K',\n" +
                "                     eventId = 'X'\n" +
                "\n" +
                "           ) as isMatch1\n" +
                "from yinew_detail\n" +
                "where deviceId = ? and timeStamp between ? AND ?\n" +
                "  and (\n" +
                "        (eventId = 'K' and properties['p5'] = 'v2')\n" +
                "        or\n" +
                "        (eventId = 'X' and properties['p3'] = 'v2')\n" +
                "\n" +
                "    )\n" +
                "group by deviceId;\n";


        EventSequenceParam sequenceParam = new EventSequenceParam("rule01", timeRangeStart, timeRangeEnd, Arrays.asList(eventParam1, eventParam2), sequenceQuerySQL);

        ruleConditions.setActionSequenceCondition(Arrays.asList(sequenceParam));
        return ruleConditions;
    }
}
