package cn.doitedu.rules.jobs;

import cn.doitedu.date.gen.LogBean;
import cn.doitedu.rules.udfs.RulesMatchFunction;
import cn.doitedu.rules.utils.FlinkUtils;
import cn.doitedu.rules.udfs.JsonToBeanFunction2;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class RulesDemo {

    public static void main(String[] args) throws Exception {

        DataStream<String> lines = FlinkUtils.createKafkaStream(args[0], SimpleStringSchema.class);

        //对数据进行预处理
        SingleOutputStreamOperator<LogBean> beanStream = lines.process(new JsonToBeanFunction2());

        beanStream.print();
        //未来要将同一个用户的最近2消息的行为保存到状态中，所以按照设备ID进行KeyBy
        KeyedStream<LogBean, String> keyedStream = beanStream.keyBy(LogBean::getDeviceId);

        //对KeyBy之后的数据进行处理
        SingleOutputStreamOperator<String> res = keyedStream.process(new RulesMatchFunction());

        res.print();

        FlinkUtils.env.execute();
    }
}
