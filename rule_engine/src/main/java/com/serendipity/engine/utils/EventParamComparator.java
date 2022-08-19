package com.serendipity.engine.utils;

import com.serendipity.engine.beans.EventBean;
import com.serendipity.engine.beans.EventParam;

import java.util.Set;

/*
    规则比较器：
        比较事件和规则是否匹配
 */
public class EventParamComparator {
    //param1是目标规则，target是实时事件
    public static boolean compare(EventParam param1, EventParam target) {
        if (param1.getEventId().equals(target.getEventId())) {
            Set<String> keys = param1.getEventProperties().keySet();
            for (String key : keys) {
                //获取规则value
                String targetValue = target.getEventProperties().get(key);
                if (!targetValue.equals(param1.getEventProperties().get(key))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    //param1是目标规则，target是实时事件
    public static boolean compare(EventParam param1, EventBean target) {
        if (param1.getEventId().equals(target.getEventId())) {
            Set<String> keys = param1.getEventProperties().keySet();
            for (String key : keys) {
                //获取规则value
                String targetValue = target.getProperties().get(key);
                //target可能为空，不是每一条数据的中key都包含在触发规则中
                if (targetValue == null || !targetValue.equals(param1.getEventProperties().get(key))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
