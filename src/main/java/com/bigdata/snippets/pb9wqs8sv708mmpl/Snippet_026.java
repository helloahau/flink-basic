// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #26
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet026 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        {"username":"zs","price":20,"event_time":"2025-07-17 10:10:10"}
        它的窗口结束时间是10:10:20秒，往前推1分钟，10:09:20，所以第一个窗口就是
        10:09:20~10:10:20   这个窗口的触发时间是10:10:23
        第二条数据是：
        {"username":"zs","price":15,"event_time":"2025-07-17 10:10:30"}
        它的时间是10:10:30 >=10:10:23 所以第一条被触发啦
    }
}
