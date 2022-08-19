package com.serendipity.engine.main;

import com.serendipity.engine.source.KafkaConsumerBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 规则系统第一版
 * 需求：
 * 获得用户事件，计算如下规则，输出结果
 * 规则：
 * 触发事件：  K事件，事件属性（ p2=v1 ）
 * 画像属性条件: tag87=v2, tag26=v1
 * 行为次数条件： 2021-06-18 ~ 当前 , 事件 C [p6=v8,p12=v5] 做过 >= 2次
 */
public class Main {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaConsumerBuilder kafkaConsumerBuilder = new KafkaConsumerBuilder();

        DataStreamSource<String> dds = env.addSource(kafkaConsumerBuilder.build("yinew_applog"));

        dds.print();


        env.execute();
    }
}
