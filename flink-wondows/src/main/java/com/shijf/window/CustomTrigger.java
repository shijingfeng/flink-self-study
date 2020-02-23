package com.shijf.window;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @description:
 * @author: sjf
 * @time: 2020/2/23 14:47
 */
@Slf4j
public class CustomTrigger extends Trigger<Tuple2<String,Integer>, TimeWindow> {

    //每个元素被添加到窗口时都会调用该方法
    @Override
    public TriggerResult onElement(Tuple2<String,Integer> s, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        log.info("======onElement====window start = {}, window end = {}", timeWindow.getStart(), timeWindow.getEnd());
        return TriggerResult.CONTINUE;
    }

    //当一个已注册的 ProcessingTime 计时器启动时调用
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        System.out.println("======onProcessingTime====");
        return TriggerResult.CONTINUE;
    }

    //当一个已注册的 EventTime 计时器启动时调用
    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        System.out.println("======onEventTime====");
        return null;
    }

    //与状态性触发器相关，当使用会话窗口时，两个触发器对应的窗口合并时，合并两个触发器的状态
    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        super.onMerge(window, ctx);
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }
}
