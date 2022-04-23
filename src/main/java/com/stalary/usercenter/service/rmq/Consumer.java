
package com.stalary.usercenter.service.rmq;

import com.alibaba.fastjson.JSONObject;
import com.stalary.usercenter.data.dto.ThreadHolder;
import com.stalary.usercenter.data.dto.UserStat;
import com.stalary.usercenter.service.StatService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer
 *
 * @author lirongqian
 * @since 2018/03/21
 */
@Slf4j
@Component
@RocketMQMessageListener(consumerGroup = "log", topic = "center_log")
public class Consumer implements RocketMQListener<Map<String, String>> {

    public static final String LOGIN_STAT = "login_stat";

    public static final String LOG = "center_log";

    private static StatService statService;

    @Autowired
    public void setStatService(StatService statService) {
        Consumer.statService = statService;
    }

    public static Map<String, ThreadHolder> map = new HashMap<>();

    @Override
    public void onMessage(Map<String, String> messageMap) {
        try {
            long startTime = System.currentTimeMillis();
            String message = messageMap.get("value");
            UserStat userStat = JSONObject.parseObject(message, UserStat.class);
            statService.saveUserStat(userStat);

            long endTime = System.currentTimeMillis();
            log.info("SubmitConsumer.time=" + (endTime - startTime));
        } catch (Exception e) {
            log.warn("light consumer error", e);
        }
    }
}
