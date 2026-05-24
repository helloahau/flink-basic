// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #24
package com.bigdata.snippets;

class pb9wqs8sv708mmplSnippet024 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        /*
         *
         * 注意：在本地运行时，默认的并行度是和你的cpu核数挂钩的，所以，为了快速看到结果，需要将并行度设置为1
         * 测试数据时，因为我们这个是eventTime,所以测试数据时，假如第一条数据是：
         * {"username":"zs","price":20,"event_time":"2025-07-17 10:10:10"}
         * 说明第一个窗口是 2025-07-17 10:10:00 ~ 2025-07-17 10:11:00
         * 因为有水印，水印时间是3秒，所以，要想触发第一个窗口有结果，必须出现条件是：
         * {"username":"zs","price":20,"event_time":"2025-07-17 10:11:03"}
         *
         * 所以第一批测试数据如下：
         * {"username":"zs","price":20,"event_time":"2025-07-17 10:10:10"}
         * {"username":"zs","price":15,"event_time":"2025-07-17 10:10:30"}
         * {"username":"zs","price":20,"event_time":"2025-07-17 10:10:40"}
         * {"username":"zs","price":20,"event_time":"2025-07-17 10:11:03"}
         *
         */
    }
}
