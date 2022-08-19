package com.serendipity.engine.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.serendipity.engine.beans.EventBean;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 将str转成object,在flink的map算子用
 */
public class Json2EventBeanMapFunction implements MapFunction<String, EventBean> {
    @Override
    public EventBean map(String s) throws Exception {

        EventBean bean = null;
        try {
            bean = JSON.parseObject(s, EventBean.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bean;
    }
}
