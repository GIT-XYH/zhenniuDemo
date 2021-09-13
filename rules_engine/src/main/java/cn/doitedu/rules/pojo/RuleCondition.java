package cn.doitedu.rules.pojo;

import lombok.Data;

@Data
public class RuleCondition {

    //规则ID
    private String ruleId;

    //触发事件
    private EventParam triggerEvent;

}
