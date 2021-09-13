package cn.doitedu.rules.udfs;

import cn.doitedu.date.gen.LogBean;
import cn.doitedu.rules.pojo.MatchResult;
import cn.doitedu.rules.pojo.RuleCondition;
import cn.doitedu.rules.service.QueryService;
import cn.doitedu.rules.utils.RulesSimulator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@Slf4j
public class RulesMatchFunction2 extends KeyedProcessFunction<String, LogBean, MatchResult> {

    private QueryService queryService;

    @Override
    public void open(Configuration parameters) throws Exception {
       ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
       queryService = new QueryService(parameterTool);
    }

    @Override
    public void processElement(LogBean event, Context ctx, Collector<MatchResult> out) throws Exception {

        //TODO 将用户的最近两小时的行为数据保存到ListState中


        //使用规则模拟器，生成一个模拟的规则
        RuleCondition condition = RulesSimulator.getRules();

        //进行规则匹配
        boolean match = queryService.isMatch(event, condition);

        if (match) {
            //输出匹配结果
            out.collect(new MatchResult(event.getEventId(), condition.getRuleId(), event.getTimeStamp(), System.currentTimeMillis()));
        }

    }

    @Override
    public void close() throws Exception {
        queryService.close();
    }
}
