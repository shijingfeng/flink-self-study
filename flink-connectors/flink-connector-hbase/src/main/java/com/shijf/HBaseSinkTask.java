package com.shijf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @description:
 * @author: sjf
 * @time: 2020/2/19 10:30
 */
public class HBaseSinkTask {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置检查点
        env.enableCheckpointing(5000);
        env.addSource(new RichSourceFunction<String>() {
            private volatile boolean isRunning = true;
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                sourceContext.collect("shijingfeng");
            }

            @Override
            public void cancel() {
            }
        }).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return "rowkey3,"+s;
            }
        }).addSink(new HBaseWrite());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
