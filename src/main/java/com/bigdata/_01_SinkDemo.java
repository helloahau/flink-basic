public class _01_SinkDemo {



    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Long> streamSource = env.fromSequence(1, 10);

        // 第一种输出方式  print
        streamSource.print();
        streamSource.print("我是字符串:");

        //5. execute-执行
        env.execute();
    }
}
