// Source: Day03-Flink四大基石 (cam5acsuc1c9pehx) snippet #11
package com.bigdata.snippets;

public class cam5acsuc1c9pehxSnippet011 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        {"action_time":1715840360,"monitor_id":"0001","camera_id":"1","car":"豫A12345","speed":50,"road_id":"01","area_id":"20"}
        {"action_time":1715840361,"monitor_id":"0001","camera_id":"1","car":"豫A12345","speed":55,"road_id":"01","area_id":"20"}
        {"action_time":1715840362,"monitor_id":"0001","camera_id":"1","car":"豫A12345","speed":60,"road_id":"01","area_id":"20"}
        {"action_time":1715840370,"monitor_id":"0001","camera_id":"1","car":"豫A12345","speed":50,"road_id":"01","area_id":"20"}

        假如你的水印时间是3秒，此时上面4条数据不会触发，需要再来一条
        {"action_time":1715840373,"monitor_id":"0001","camera_id":"1","car":"豫A12345","speed":50,"road_id":"01","area_id":"20"}

        假如你的程序已经运行过2025年的数据了，这个窗口已经计算到了2025年，2024年的窗口早就关闭了，你运行上面的数据是没有效果的，需要重新启动一下flink程序。
    }
}
