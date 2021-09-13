package cn.doitedu.rules.pojo;

import lombok.Data;

import java.util.Map;

/**
 * 封装事件触发条件的Bean
 */

@Data
public class EventParam {

    //预设的时间ID：K
    private String eventId;

    //预设的行为属性：p2=v1， p3 = v3
    private Map<String, String> properties;
}
