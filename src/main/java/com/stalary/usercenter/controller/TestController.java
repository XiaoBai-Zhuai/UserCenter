package com.stalary.usercenter.controller;

import com.stalary.lightmqclient.facade.Producer;
import com.stalary.usercenter.data.dto.ResponseMessage;
import com.stalary.usercenter.service.MailService;
import com.stalary.usercenter.service.UserService;
import com.stalary.usercenter.service.rmq.Consumer;
import com.stalary.usercenter.data.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

/**
 * TestController
 *
 * @author lirongqian
 * @since 2018/03/26
 */
@RestController
@Slf4j
public class TestController {

    @Resource
    private Producer producer;

    @Resource
    private MailService mailService;

    @Resource
    private UserService userService;

    @GetMapping("/sendKafka")
    public ResponseMessage testKafka(
            @RequestParam String message) {
        producer.send("test", message);
        return ResponseMessage.successMessage();
    }

    @GetMapping("/sendMail")
    public ResponseMessage testMail(
            @RequestParam String address) {
        mailService.sendSimpleMail(address);
        return ResponseMessage.successMessage();
    }

    @GetMapping("/sendLog")
    public ResponseMessage sendLog() {
        log.warn("user_log" + Constant.SPLIT + Constant.PROJECT + Constant.SPLIT + 1 + Constant.SPLIT + "项目验证密钥" + 123214 + "失败");
        return ResponseMessage.successMessage();
    }

    @GetMapping("/kafkaState")
    public ResponseMessage getKafka() {
        return ResponseMessage.successMessage(Consumer.map.get(Constant.KAFKA_INFO));
    }

    @GetMapping("/address")
    public ResponseMessage getAddress(
            HttpServletRequest request) {
        String ip = userService.getIp(request);
        return ResponseMessage.successMessage(userService.getAddress(ip));
    }
}