package cn.doitedu.rules.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 规则匹配后输出的结果
 */

@Data
@AllArgsConstructor
public class MatchResult {

    private String eventId;

    private String ruleId;

    private long eventTime;

    private long triggerTime;


}
