package com.bigdata.day03.sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

import java.sql.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Student{
    private int id;
    private String name;
    private int age;

}
class _04JDBCSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Student> dataStreamSource = env.fromElements(
                new Student(1, "张三", 18),
                new Student(2, "李四", 19),
                new Student(3, "王五", 20)
        );

        // Flink 2.x: JdbcSink.sink() returns 1.x SinkFunction, incompatible with addSink(); use inline RichSinkFunction
        dataStreamSource.addSink(new RichSinkFunction<Student>() {
            private transient Connection conn;
            private transient PreparedStatement ps;

            @Override
            public void open(OpenContext context) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://node01:3306/gongcheng", "root", "123456");
                ps = conn.prepareStatement("insert into stu values(?,?,?)");
            }

            @Override
            public void invoke(Student student, Context context) throws Exception {
                ps.setInt(1, student.getId());
                ps.setString(2, student.getName());
                ps.setInt(3, student.getAge());
                ps.executeUpdate();
            }

            @Override
            public void close() throws Exception {
                if (ps != null) ps.close();
                if (conn != null) conn.close();
            }
        });

        env.execute();
    }
}
