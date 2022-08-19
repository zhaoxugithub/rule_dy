package com.serendipity.engine.function;

import com.serendipity.engine.beans.EventBean;
import com.serendipity.engine.beans.RuleConditions;
import com.serendipity.engine.beans.RuleMatchResult;
import com.serendipity.engine.main.EventParamComparator;
import com.serendipity.engine.main.RuleMonitor;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/*
 * 规则系统第一版
 * 需求：
 * 获得用户事件，计算如下规则，输出结果
 * 规则：
 * 触发事件：  K事件，事件属性（ p2=v1 ）
 * 画像属性条件: tag87=v2, tag26=v1
 * 行为次数条件： 2021-06-18 ~ 当前 , 事件 C [p6=v8,p12=v5] 做过 >= 2次
 * 行为序列
    处理过程：
    每来一条数据event，进行规则触发事件判断，如果不是就返回
        如果是就去hbase里面是查询画像属性条件，如果不是就返回
            如果满足就去查询ck中的行为次数事件是否满足，满足就记录，不满足就返回
 */
//<String, EventBean, RuleMatchResult> String是key,EventBean是输入类型，RuleMatchResult是输出类型
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, EventBean, RuleMatchResult> {
    @Override
    public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, RuleMatchResult>.Context context, Collector<RuleMatchResult> collector) throws Exception {
        //获取一个规则
        RuleConditions rule = RuleMonitor.getRule();
        //判断当前事件是否满足规则定义的触发事件条件
        if (!EventParamComparator.compare(rule.getTriggerEvent(), eventBean)) return;
        //TODO 查询画像条件是否满足
        //TODO 行为次数条件是否满足
        //TODO 行为序列是否满足
        //TODO 模拟随机命中
        if (RandomUtils.nextInt(1, 10) % 3 == 0) {
            RuleMatchResult matchResult = new RuleMatchResult(eventBean.getDeviceId(), rule.getRuleId(), eventBean.getTimeStamp(), System.currentTimeMillis());
            collector.collect(matchResult);
        }
    }
}
