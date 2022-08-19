package com.serendipity.engine.function;

import com.serendipity.engine.beans.EventBean;
import com.serendipity.engine.beans.EventParam;
import com.serendipity.engine.beans.RuleConditions;
import com.serendipity.engine.beans.RuleMatchResult;
import com.serendipity.engine.queryservice.ClickHouseQueryServiceImpl;
import com.serendipity.engine.queryservice.HbaseQueryServiceImpl;
import com.serendipity.engine.utils.ConnectionUtils;
import com.serendipity.engine.utils.EventParamComparator;
import com.serendipity.engine.utils.RuleSimulator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

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
@Slf4j
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, EventBean, RuleMatchResult> {

    private ClickHouseQueryServiceImpl clickHouseQueryService = null;

    private HbaseQueryServiceImpl hbaseQueryService = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取一个Hbase连接
        hbaseQueryService = new HbaseQueryServiceImpl(ConnectionUtils.getHbaseConnection());
        clickHouseQueryService = new ClickHouseQueryServiceImpl(ConnectionUtils.getClickhouseConnection());
    }

    @Override
    public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, RuleMatchResult>.Context context, Collector<RuleMatchResult> collector) throws Exception {
        log.debug("收到一条事件数据:eventId={},deviceId={},properties={}", eventBean.getEventId(), eventBean.getDeviceId(), eventBean.getProperties());
        //获取一个规则
        RuleConditions rule = RuleSimulator.getRule();
        //判断当前事件是否满足规则定义的触发事件条件
        if (!EventParamComparator.compare(rule.getTriggerEvent(), eventBean)) return;
        log.debug("触发条件规则通过，满足事件id={}", "k");
        //查询画像条件是否满足
        Map<String, String> userProfileConditions = rule.getUserProfileConditions();
        boolean queryProfileCondition = false;
        if (userProfileConditions != null) {
            queryProfileCondition = hbaseQueryService.queryProfileCondition(eventBean.getDeviceId(), userProfileConditions);
            //如果不满足画像规则条件则整个规则计算就退出
            if (!queryProfileCondition) return;
        }
        //TODO 行为次数条件是否满足
        List<EventParam> actionCountConditions = rule.getActionCountConditions();
        if (actionCountConditions != null && actionCountConditions.size() > 0) {
            for (EventParam actionCountCondition : actionCountConditions) {
                long clickHouseCount = clickHouseQueryService.queryEventCountCondition(actionCountCondition, eventBean.getDeviceId(), actionCountCondition.getQuerySql());
                //如果不满足就返回
                if (clickHouseCount < actionCountCondition.getCountThreshold()) return;
            }
        }
        //TODO 行为序列是否满足
        //TODO 模拟随机命中
        RuleMatchResult matchResult = new RuleMatchResult(eventBean.getDeviceId(), rule.getRuleId(), eventBean.getTimeStamp(), System.currentTimeMillis());
        collector.collect(matchResult);
    }
}
