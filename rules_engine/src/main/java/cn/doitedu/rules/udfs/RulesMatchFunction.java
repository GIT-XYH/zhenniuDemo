package cn.doitedu.rules.udfs;

import cn.doitedu.date.gen.LogBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.directory.api.util.GeneralizedTime;
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
public class RulesMatchFunction extends KeyedProcessFunction<String, LogBean, String> {

    private Connection hbaseConn;

    private java.sql.Connection clickHouseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建Hbase连接
        org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "node-1.51doit.cn,node-2.51doit.cn,node-2.51doit.cn");
        hbaseConf.setInt("hbase.zookeeper.property.clientPort", 2181);
        hbaseConn = ConnectionFactory.createConnection(hbaseConf);

        //创建ClickHouse连接
        clickHouseConn = DriverManager.getConnection("jdbc:clickhouse://node-3.51doit.cn:8123/default?characterEncoding=utf-8");

    }

    @Override
    public void processElement(LogBean event, Context ctx, Collector<String> out) throws Exception {


        String res = "No";
        //1.匹配数据的事件（触发事件：K事件，并且事件属性（p2 = v1））
        String eventId = event.getEventId();
        if (!"K".equals(eventId) || !"v1".equals(event.getProperties().get("p2"))) {
            return;
        }

        //2.查询Hbase匹配画像标签：f列族 tag87=v2 and tag26=v1
        Table table = hbaseConn.getTable(TableName.valueOf("zenniu_profile"));
        Get get  = new Get(Bytes.toBytes(event.getDeviceId()));
        //指定查询的列族和列的标识符
        get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("tag87"));
        get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("tag26"));
        Result result = table.get(get);
        if(result == null || !"v1".equals(new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("tag87")))) || !"v1".equals(new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("tag26"))))) {
            return;
        }
        table.close();
        log.debug(event.getDeviceId() + ": 已经匹配了画像标签");

        //3.查询ClickHouse匹配历史行为数据：2021-09-01 00:00:00 ~ 2021-09-06 23:59:59 事件C （p6=v8， p12=v5） 做过 >= 2次
        // select count(1) from zenniu_detail where eventId = 'K' and properties['p1'] = 'v2' and properties['p5'] = 'v1' and timeStamp >= 1630425600000 and  timeStamp <= 1630943999000;
        String sql = "select count(1) from zenniu_detail where deviceId = ? and eventId = ? and properties['p1'] = ? and properties['p5'] = ? and timeStamp >= ? and  timeStamp <= ?";
        PreparedStatement pstm = clickHouseConn.prepareStatement(sql);
        pstm.setString(1, event.getDeviceId());
        pstm.setString(2, "K");
        pstm.setString(3, "v2");
        pstm.setString(4, "v1");
        pstm.setLong(5, 1630425600000L);
        pstm.setLong(6,1630943999000L);
        ResultSet resultSet = pstm.executeQuery();
        if (resultSet.next() && resultSet.getInt(1) >= 2) {
            res = "OK";
        } else {
            return;
        }
        resultSet.close();
        pstm.close();


        //如果匹配上，输出数据，触发规则
        out.collect(res);

    }

    @Override
    public void close() throws Exception {
        hbaseConn.close();
        clickHouseConn.close();
    }
}
