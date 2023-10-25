package com.furkankayam.mqttbasic.controller;

import com.furkankayam.mqttbasic.config.MqttConfig;
import com.furkankayam.mqttbasic.exception.ExceptionMessages;
import com.furkankayam.mqttbasic.model.MqttPublishModel;
import com.furkankayam.mqttbasic.model.MqttSubscribeModel;
import jakarta.validation.Valid;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping(value = "/api/mqtt")
public class MqttController {

    @PostMapping("/publish")
    public ResponseEntity<Void> publishMessage(@RequestBody @Valid MqttPublishModel messagePublishModel,
                                               BindingResult bindingResult) throws org.eclipse.paho.client.mqttv3.MqttException {
        if (bindingResult.hasErrors()) {
            throw new RuntimeException(ExceptionMessages.SOME_PARAMETERS_INVALID);
        }

        MqttMessage mqttMessage = new MqttMessage(messagePublishModel.getMessage().getBytes());
        mqttMessage.setQos(messagePublishModel.getQos());
        mqttMessage.setRetained(messagePublishModel.getRetained());

        MqttConfig.getInstance().publish(messagePublishModel.getTopic(), mqttMessage);

        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/subscribe")
    public ResponseEntity<List<MqttSubscribeModel>> subscribeChannel(@RequestParam(value = "topic") String topic,
                                                                     @RequestParam(value = "wait_millis") Integer waitMillis)
            throws InterruptedException, org.eclipse.paho.client.mqttv3.MqttException {
        List<MqttSubscribeModel> messages = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        MqttConfig.getInstance().subscribeWithResponse(topic, (s, mqttMessage) -> {
            MqttSubscribeModel mqttSubscribeModel = new MqttSubscribeModel();
            mqttSubscribeModel.setId(mqttMessage.getId());
            mqttSubscribeModel.setMessage(new String(mqttMessage.getPayload()));
            mqttSubscribeModel.setQos(mqttMessage.getQos());
            messages.add(mqttSubscribeModel);
            countDownLatch.countDown();
        });

        countDownLatch.await(waitMillis, TimeUnit.MILLISECONDS);

        return new ResponseEntity<>(messages,HttpStatus.OK);
    }


}

