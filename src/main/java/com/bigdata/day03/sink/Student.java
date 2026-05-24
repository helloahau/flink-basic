package com.bigdata.day03.sink;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@Data
@NoArgsConstructor
@AllArgsConstructor
class Student{
    private int id;
    private String name;
    private int age;
}
public class _04JDBCSink {

    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Student> dataStreamSource = env.fromElements(
                new Student(1, "张三", 18),
                new Student(2, "李四", 19),
                new Student(3, "王五", 20)
        );

        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUrl("jdbc:mysql://node01:3306/gongcheng")
                .withUsername("root").withPassword("123456").build();
        // 将数据写入到mysql中
        dataStreamSource.addSink(JdbcSink.sink(
                "insert into stu values(?,?,?)", new JdbcStatementBuilder<Student>() {
                    @Override
                    public void accept(PreparedStatement stat, Student student) throws SQLException {
                        stat.setInt(1, student.getId());
                        stat.setString(2, student.getName());
                        stat.setInt(3, student.getAge());
                    }
                }, jdbcConnectionOptions

        ));

        env.execute();
    }
}
