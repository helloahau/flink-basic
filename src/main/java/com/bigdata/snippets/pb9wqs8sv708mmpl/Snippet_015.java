// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #15
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet015 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        #if (${PACKAGE_NAME} && ${PACKAGE_NAME} != "")package ${PACKAGE_NAME};#end
        #parse("File Header.java")
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
        import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;                          
        /**
         @基本功能:
         @program:${PROJECT_NAME}
         @author: 闫哥
         @create:${YEAR}-${MONTH}-${DAY} ${HOUR}:${MINUTE}:${SECOND}
        **/
        public class ${NAME} {

            public static void main(String[] args) throws Exception {

            //1. env-准备环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
            // 获取tableEnv对象
           // 通过env 获取一个table 环境
           StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            //2. 创建表对象
            //3. 编写sql语句
            //4. 将Table变为stream流





             //5. execute-执行
            env.execute();
          }
        }
    }
}
