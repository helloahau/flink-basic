package com.bigdata.day03;

import com.bigdata.day03.sink.MyJdbcSink;
import com.bigdata.day03.sink.Student;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-08-14 09:09:58
 **/
public class Demo02 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> dataStreamSource = env.fromElements(

                new Student(1, "张三三", 18),
                new Student(2, "李四四", 19),
                new Student(3, "王五五", 20)

        );



        dataStreamSource.addSink(new MyJdbcSink());

        //5. execute-执行
        env.execute();
    }
}
