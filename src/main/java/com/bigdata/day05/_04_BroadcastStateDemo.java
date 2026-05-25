package com.bigdata.day05;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Broadcast State（广播状态）演示
 *
 * 应用场景：动态规则下发
 * - 主流（数据流）：实时事件/日志等业务数据
 * - 广播流（规则流）：低吞吐量的规则/配置，需要广播到所有并行实例
 *
 * 本示例：
 * - 主流: 用户行为日志 "userId,action"
 * - 广播流: 动态过滤规则 "ruleName,actionKeyword"
 * - 逻辑: 对每条日志按当前规则中的关键词进行匹配过滤输出
 */
public class _04_BroadcastStateDemo {

    // 广播状态描述符：规则名 → 关键词
    static final MapStateDescriptor<String, String> RULE_STATE_DESC =
            new MapStateDescriptor<>("rules", String.class, String.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 主流：用户行为
        DataStream<String> userStream = env.fromData(
                "user1,click",
                "user2,buy",
                "user3,click",
                "user4,login",
                "user5,buy",
                "user6,logout"
        );

        // 规则流（低吞吐，广播）：ruleName → 要匹配的 action 关键词
        DataStream<String> ruleStream = env.fromData(
                "rule1,click",   // 规则1: 匹配 click 行为
                "rule2,buy"      // 规则2: 匹配 buy 行为
        );

        // 将规则流转为广播流
        BroadcastStream<String> broadcastRuleStream = ruleStream.broadcast(RULE_STATE_DESC);

        // 主流 connect 广播流 → BroadcastProcessFunction
        DataStream<String> result = userStream
                .connect(broadcastRuleStream)
                .process(new DynamicFilterFunction());

        result.print("匹配结果");

        env.execute("Broadcast State Demo");
    }

    /**
     * BroadcastProcessFunction:
     * - processBroadcastElement: 更新广播状态中的规则
     * - processElement: 用当前所有规则对主流数据进行匹配
     */
    static class DynamicFilterFunction
            extends BroadcastProcessFunction<String, String, String> {

        @Override
        public void processBroadcastElement(String ruleStr,
                Context ctx, Collector<String> out) throws Exception {
            // 解析规则: "ruleName,actionKeyword"
            String[] parts = ruleStr.split(",");
            BroadcastState<String, String> state = ctx.getBroadcastState(RULE_STATE_DESC);
            state.put(parts[0], parts[1]);
            System.out.println("规则已更新: " + parts[0] + " → 匹配关键词: " + parts[1]);
        }

        @Override
        public void processElement(String userLog,
                ReadOnlyContext ctx, Collector<String> out) throws Exception {
            // 解析用户日志: "userId,action"
            String[] parts = userLog.split(",");
            String action = parts[1];

            ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(RULE_STATE_DESC);
            // 遍历所有规则，匹配则输出
            for (java.util.Map.Entry<String, String> entry : state.immutableEntries()) {
                if (action.contains(entry.getValue())) {
                    out.collect(String.format("[%s] 用户 %s 触发规则 %s (action=%s)",
                            entry.getKey(), parts[0], entry.getKey(), action));
                }
            }
        }
    }
}

