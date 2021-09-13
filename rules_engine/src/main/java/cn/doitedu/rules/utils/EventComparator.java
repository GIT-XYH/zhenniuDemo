package cn.doitedu.rules.utils;

import cn.doitedu.date.gen.LogBean;
import cn.doitedu.rules.pojo.EventParam;

import java.util.Map;
import java.util.Set;

/**
 * 输入的事件和预设的事件的比较工具类
 */
public class EventComparator {


    public static boolean compare(LogBean event, EventParam triggerEvent) {

        if(triggerEvent.getEventId().equals(event.getEventId())) {
            //预设的要比较行为属性
            //{(p2, v1), (p3, v3)}
            Map<String, String> properties = triggerEvent.getProperties();
            //[p2,p3]
            Set<String> keys = properties.keySet();
            for (String key : keys) {
                //取出当前输入事件中实际的属性值
                String inputValue = event.getProperties().get(key);
                //预设的值
                String targetValue = properties.get(key);
                if (!targetValue.equals(inputValue)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }
}
