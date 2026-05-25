package com.bigdata.day05;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * KeyedState 全览演示：ValueState / ListState / MapState
 *
 * 三种最常用的键控状态类型：
 * 1. ValueState<T>   — 单值状态（最简单）
 * 2. ListState<T>    — 列表状态（追加元素）
 * 3. MapState<K,V>   — 映射状态（key-value 查找/更新）
 *
 * 本示例用 (userId, page) 数据模拟用户浏览行为，分别用三种状态统计：
 * - ValueState:  每个用户的浏览总次数
 * - ListState:   每个用户最近浏览过的页面列表
 * - MapState:    每个用户各页面的浏览次数（page → count）
 */
public class _05_AllKeyedStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // (userId, page)
        DataStream<Tuple2<String, String>> stream = env.fromData(
                Tuple2.of("alice", "home"),
                Tuple2.of("bob",   "product"),
                Tuple2.of("alice", "product"),
                Tuple2.of("bob",   "home"),
                Tuple2.of("alice", "cart"),
                Tuple2.of("alice", "product"),
                Tuple2.of("bob",   "product")
        );

        stream.keyBy(t -> t.f0)
                .process(new AllStateDemo())
                .print();

        env.execute("All KeyedState Demo");
    }

    static class AllStateDemo
            extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

        private ValueState<Long>            totalCount;   // 总浏览次数
        private ListState<String>           pageHistory;  // 浏览历史列表
        private MapState<String, Long>      pageCounter;  // 各页面浏览次数

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext ctx) {
            totalCount  = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("total", Long.class));
            pageHistory = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("history", String.class));
            pageCounter = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("counter", String.class, Long.class));
        }

        @Override
        public void processElement(Tuple2<String, String> in,
                                   Context ctx, Collector<String> out) throws Exception {
            String user = in.f0;
            String page = in.f1;

            // 1. ValueState: 累计总数
            Long total = totalCount.value();
            total = (total == null ? 0L : total) + 1;
            totalCount.update(total);

            // 2. ListState: 追加页面
            pageHistory.add(page);
            List<String> history = new ArrayList<>();
            pageHistory.get().forEach(history::add);

            // 3. MapState: page → count
            Long cnt = pageCounter.get(page);
            cnt = (cnt == null ? 0L : cnt) + 1;
            pageCounter.put(page, cnt);

            out.collect(String.format(
                    "用户[%s] 访问[%s] | 总计=%d | 历史=%s | 各页计数=%s",
                    user, page, total, history, mapStateSnapshot(pageCounter)));
        }

        private String mapStateSnapshot(MapState<String, Long> ms) throws Exception {
            StringBuilder sb = new StringBuilder("{");
            ms.entries().forEach(e -> sb.append(e.getKey()).append(":").append(e.getValue()).append(","));
            if (sb.length() > 1) sb.deleteCharAt(sb.length() - 1);
            sb.append("}");
            return sb.toString();
        }
    }
}

