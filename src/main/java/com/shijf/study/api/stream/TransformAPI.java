package com.shijf.study.api.stream;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class TransformAPI {
    public static void main(String[] args) throws Exception {
        //创建流上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取socket数据形成数据流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 8888);

        //map  输入一个元素，然后返回一个元素，中间可以做一些清洗转换等操作
        SingleOutputStreamOperator<String> map = stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String txt) throws Exception {
                txt = txt+"love";
                return txt;
            }
        });

        //flatmap：输入一个元素，可以返回零个，一个或者多个元素
        stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out)
                throws Exception {
                for(String word: value.split(" ")){
                    out.collect(word);
                }
            }
        });


        //filter：过滤函数，对传入的数据进行判断，符合条件的数据会被留下
        stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return Integer.valueOf(value) != 0;
            }
        });

        //keyBy：根据指定的key进行分组，相同key的数据会进入同一个分区
        stream.keyBy("someKey"); // 指定对象中的 "someKey"字段作为分组key
        stream.keyBy(0); //指定Tuple中的第一个元素作为分组key
        //注意：以下类型是无法作为key的
        //1：一个实体类对象，没有重写hashCode方法，并且依赖object的hashCode方法
        //2：一个任意形式的数组类型
        //3：基本数据类型，int，long

        //reduce：将当前元素与最后一个减少的值组合在一起，并发出新的值。必须在KeyedStream（keyBy孙子会将DataStream转化为keyedstream）上操作
        stream.keyBy(0).reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String value1, String value2)
                    throws Exception {
                return value1 + value2;
            }
        });

        //flod：将当前元素与最后一个折叠值组合并发出新值。必须在KeyedStream（keyBy孙子会将DataStream转化为keyedstream）上操作
        stream.keyBy(0).fold("start", new FoldFunction<String, String>() {
                    @Override
                    public String fold(String current, String value) {
                        return current + "-" + value;
                    }
                });

        //aggregations：sum(),min(),max()，minby(),maxby()等
        //在keyed数据流上滚动聚合。min和minBy的区别在于，min返回最小值，而minBy返回该字段中具有最小值的元素(max和maxBy也是如此)。
        KeyedStream<String, Tuple> keyedStream = stream.keyBy(0);
        keyedStream.sum(0);
        keyedStream.sum("key");
        keyedStream.min(0);
        keyedStream.min("key");
        keyedStream.max(0);
        keyedStream.max("key");
        keyedStream.minBy(0);
        keyedStream.minBy("key");
        keyedStream.maxBy(0);
        keyedStream.maxBy("key");

        //union 合并多个流，新的流会包含所有流中的数据，但是union是一个限制，就是所有合并的流类型必须是一致的。
        stream.union(stream2);

        //Window:Window 函数允许按时间或其他条件对现有 KeyedStream 进行分组。 以下是以 5 秒的时间窗口聚合：
        stream.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //WindowAll:WindowAll 将元素按照某种特性聚集在一起，该函数不支持并行操作，默认的并行度就是1，所以如果使用这个算子的话需要注意一下性能问题，
        stream.keyBy(0).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //split:此功能根据条件将流拆分为两个或多个流。 当你获得混合流然后你可能希望单独处理每个数据流时，:可以使用此方法。
        SplitStream<String> split = stream.split(new OutputSelector<String>() {
            @Override
            public Iterable<String> select(String value) {
                List<String> output = new ArrayList<String>();
                if (value.length() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        });

        //select:Split 算子将数据流拆分成两个数据流（奇数、偶数），接下来你可能想从拆分流中选择特定流，那么就得搭配使用 Select 算子（一般这两者都是搭配在一起使用的）
        DataStream<String> even = split.select("even");
        DataStream<String> odd = split.select("odd");
        DataStream<String> all = split.select("even","odd");

        env.execute("Flink Transform ");
    }
}
