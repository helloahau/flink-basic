package com.bigdata.day05;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * KeyedProcessFunction + Timer（定时器）演示
 *
 * KeyedProcessFunction 是 Flink 最底层的算子，提供：
 * 1. 访问 KeyedState（键控状态）
 * 2. 注册 ProcessingTime / EventTime 定时器
 * 3. 定时器触发时执行 onTimer() 回调
 *
 * 本示例：统计每个 key 在 5 秒内的累计次数，5 秒到期后打印并清零
 */
public class _02_KeyedProcessFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 模拟输入流：(key, value)
        DataStream<Tuple2<String, Integer>> stream = env.fromData(
                Tuple2.of("a", 1),
                Tuple2.of("b", 1),
                Tuple2.of("a", 1),
                Tuple2.of("a", 1),
                Tuple2.of("b", 1),
                Tuple2.of("c", 1)
        );

        stream.keyBy(t -> t.f0)
                .process(new CountWithTimerFunction())
                .print();

        env.execute("KeyedProcessFunction Timer Demo");
    }

    /**
     * 自定义 KeyedProcessFunction:
     * - 累计每个 key 的计数
     * - 首次来数据时注册一个 5 秒后的 ProcessingTime 定时器
     * - 定时器触发时输出结果并清除状态
     */
    static class CountWithTimerFunction
            extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {

        // 状态：当前 key 的累计计数
        private ValueState<Long> countState;
        // 状态：已注册的定时器时间戳（避免重复注册）
        private ValueState<Long> timerState;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("count", Long.class));
            timerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timer", Long.class));
        }

        @Override
        public void processElement(Tuple2<String, Integer> value,
                                   Context ctx, Collector<String> out) throws Exception {
            // 更新计数
            Long count = countState.value();
            count = (count == null ? 0L : count) + value.f1;
            countState.update(count);

            // 首次来数据时注册 5 秒后的定时器
            if (timerState.value() == null) {
                long fireTime = ctx.timerService().currentProcessingTime() + 5000L;
                ctx.timerService().registerProcessingTimeTimer(fireTime);
                timerState.update(fireTime);
                out.collect(String.format("[%s] 计数更新: %d, 已注册定时器 (5 秒后触发)", value.f0, count));
            } else {
                out.collect(String.format("[%s] 计数更新: %d", value.f0, count));
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发：输出最终结果，清除状态
            Long finalCount = countState.value();
            out.collect(String.format("⏰ 定时器触发 [key=%s]: 5 秒内共 %d 条数据 → 清零", ctx.getCurrentKey(), finalCount));
            countState.clear();
            timerState.clear();
        }
    }
}

