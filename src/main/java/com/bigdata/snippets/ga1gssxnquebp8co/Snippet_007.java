// Source: Day04-Flink四大基石2 (ga1gssxnquebp8co) snippet #7
package com.bigdata.snippets;

public class ga1gssxnquebp8coSnippet007 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        //开启checkpoint，默认是无限重启，可以设置为不重启
        //env.setRestartStrategy(RestartStrategies.noRestart());

        //重启3次，重启时间间隔是10s
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        //2分钟内重启3次，重启时间间隔是5s
        env.setRestartStrategy(
            RestartStrategies.failureRateRestart(3,
                                                 Time.of(2,TimeUnit.MINUTES),
                                                 Time.of(5,TimeUnit.SECONDS))
        );


        env.execute("checkpoint自动重启");   //最后一句execute可以设置jobName,显示在8081界面
    }
}
