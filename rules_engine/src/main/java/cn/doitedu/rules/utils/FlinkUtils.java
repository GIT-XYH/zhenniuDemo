package cn.doitedu.rules.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtils {


    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T> createKafkaStream(String path, Class<? extends DeserializationSchema<T>> clazz) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(path);
        //将ParameterTool解析的参数设置为全局参数（TaskManager中的每个subtask都可以使用这些参数）
        env.getConfig().setGlobalJobParameters(parameterTool);
        String bootstrapServers = parameterTool.getRequired("bootstrap.servers");

        //为了容错，要开启checkpoint
        env.enableCheckpointing(parameterTool.getLong("checkpoint.interval", 60000));
        //在checkpoint时，将状态保存到HDFS存储后端
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");
        //将job cancel后，保留外部存储的checkpoint数据（为了以后重新提交job恢复数据）
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));
        properties.setProperty("auto.offset.reset", parameterTool.get("auto.offset.reset", "earliest"));
        //properties.setProperty("group.id", parameterTool.get("group.id", "g001"));

        String topics = parameterTool.getRequired("input.topics");
        List<String> topicList = Arrays.asList(topics.split(","));

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(
                topicList,
                clazz.newInstance(),
                properties
        );

        //在checkpoint成功后，不将偏移量写入到Kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);
    }


}
