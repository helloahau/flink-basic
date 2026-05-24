package com.bigdata.day03.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @create:2025-04-17 15:43:52
 **/
public class MyJdbcSink extends RichSinkFunction<Student>{

    Connection conn = null;
    PreparedStatement ps = null;
    @Override
    public void invoke(Student student, Context context) throws Exception {

        ps.setInt(1,student.getId());
        ps.setString(2,student.getName());
        ps.setInt(3,student.getAge());
        ps.execute();
        //conn.commit();
    }



    @Override
    public void open(OpenContext context) throws Exception {

        // 这个里面可以编写连接数据库的代码
        // 这个里面编写连接数据库的代码
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://node01:3306/gongcheng", "root", "123456");
        ps = conn.prepareStatement("INSERT INTO `stu` (`id`, `name`, `age`) VALUES (?, ?, ?)");

    }

    @Override
    public void close() throws Exception {
        ps.close();
        conn.close();

    }
}

class _07CustomSinkDemo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2. source-加载数据
        DataStreamSource<Student> dataStreamSource = env.fromElements(new Student(5, "特没谱", 90));
        //3. transformation-数据处理转换
        //4. sink-数据输出
        dataStreamSource.addSink(new MyJdbcSink());


        //5. execute-执行
        env.execute();
    }
}
