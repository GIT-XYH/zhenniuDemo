package cn.doitedu.rules.service;

import cn.doitedu.date.gen.LogBean;
import cn.doitedu.rules.pojo.EventParam;
import cn.doitedu.rules.pojo.RuleCondition;
import cn.doitedu.rules.utils.EventComparator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

public class QueryService {


    private Connection hbaseConn;

    private java.sql.Connection clickHouseConn;

    public QueryService(ParameterTool parameters) {

        try {
            //创建Hbase连接
            org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", parameters.getRequired("hbase.url"));
            hbaseConf.set("hbase.zookeeper.property.clientPort", parameters.getRequired("hbase.port"));
            hbaseConn = ConnectionFactory.createConnection(hbaseConf);
            //创建ClickHouse连接
            clickHouseConn = DriverManager.getConnection(parameters.getRequired("clickhouse.url"));
        } catch (IOException e1) {
            throw new RuntimeException("建立Hbase连接出现异常", e1);
        } catch (SQLException e2) {
            throw new RuntimeException("建立ClickHouse连接持续异常",e2);
        }

    }


    public void close() throws Exception {
        hbaseConn.close();
        clickHouseConn.close();
    }

    public boolean isMatch(LogBean event, RuleCondition condition) {
        //LogBean event用户当前的行为数据
        //RuleCondition condition 事先预设好的规则

        //1.触发的行为：eventId：K，属性： p2 = v1 （0个或多个属性）
        EventParam triggerEvent = condition.getTriggerEvent();
        boolean isTrigger = EventComparator.compare(event, triggerEvent);
        if (!isTrigger) {
            return false;
        }

        //2.用户画像条件：deviceId： 000019  画像标签：tag11 = v5， tag89 = v1, tag99 = v2


        //3.历史行为数据: SQL, 时间段，属性properties['p1'] = v1, properties['p2'] = v3


        return true;
    }
}
