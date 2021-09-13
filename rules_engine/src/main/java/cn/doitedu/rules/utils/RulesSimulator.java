package cn.doitedu.rules.utils;

import cn.doitedu.rules.pojo.EventParam;
import cn.doitedu.rules.pojo.RuleCondition;
import org.apache.commons.collections.map.HashedMap;

import java.util.HashMap;

/**
 * 获取一个模拟的规则，以后是使用Drools规则引擎的规则
 */
public class RulesSimulator {

    public static RuleCondition getRules() {

        //模拟一个规则
        RuleCondition condition = new RuleCondition();
        condition.setRuleId("my-rule-00001");

        //设置触发的事件，预设一些条件
        EventParam eventParam = new EventParam();
        //预设触发事件为K
        eventParam.setEventId("K");
        //预设条件属性
        HashMap<String, String> eventProps = new HashMap<String, String>();
        eventProps.put("p2", "v1");
        eventProps.put("p3", "v3");
        eventParam.setProperties(eventProps);

        condition.setTriggerEvent(eventParam);

        return null;
    }
}
