package cn.doitedu.rules.udfs;

import cn.doitedu.date.gen.LogBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class JsonToBeanFunction2 extends ProcessFunction<String, LogBean> {

    @Override
    public void processElement(String value, Context ctx, Collector<LogBean> out) throws Exception {

        try {
            LogBean logBean = JSON.parseObject(value, LogBean.class);
            out.collect(logBean);
        } catch (Exception e) {
            //e.printStackTrace();
            //log.warn("parse json error： " + value);
        }


    }


}
