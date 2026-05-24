// Source: Day02-Flink-普通API的使用 (ves1cik0ui24swqt) snippet #3
package com.bigdata.snippets;

public class ves1cik0ui24swqtSnippet003 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        #if (${PACKAGE_NAME} && ${PACKAGE_NAME} != "")package ${PACKAGE_NAME};#end
        #parse("File Header.java")
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

            //2. source-加载数据
            //3. transformation-数据处理转换
            //4. sink-数据输出

             //5. execute-执行
            env.execute();
          }
        }
    }
}
