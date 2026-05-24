package com.bigdata.schema;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


/**
 * @基本功能:
 * @program:FlinkProject
 * @create:2025-12-22 11:01:57
 **/
public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {

    // SourceRecord{sourcePartition={server=mysql_binlog_source},
    // sourceOffset={transaction_id=null, ts_sec=1703172018,
    // file=binlog.000024, pos=235, row=1, server_id=1, event=2}}
    // ConnectRecord{topic='mysql_binlog_source.cdc_test.user_info',
    // kafkaPartition=null, key=Struct{id=1},
    // keySchema=Schema{mysql_binlog_source.cdc_test.user_info.Key:STRUCT},
    // value=Struct{after=Struct{id=1,name=zhangsan,age=10},
    // source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1703172018000,db=cdc_test,
    // table=user_info,server_id=1,file=binlog.000024,pos=382,row=0},op=c,ts_ms=1703213680758},
    // valueSchema=Schema{mysql_binlog_source.cdc_test.user_info.Envelope:STRUCT}, timestamp=null,
    // headers=ConnectHeaders(headers=)}

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        // 将每一个SourceRecord【binlog原生的】 变为 json字符串
        //创建JSON对象用于封装结果数据
        JSONObject result = new JSONObject();

        //获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db", fields[1]);
        result.put("tableName", fields[2]);

        //获取before数据
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            //获取列信息
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                beforeJson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJson);

        //获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            //获取列信息
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                afterJson.put(field.name(), after.get(field));
            }
        }
        result.put("after", afterJson);

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);

        //输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
