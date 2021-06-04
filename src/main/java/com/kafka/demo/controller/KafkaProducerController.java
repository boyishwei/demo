package com.kafka.demo.controller;

import com.kafka.demo.service.KafKaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaProducerController
{
    private final KafKaProducerService producerService;


    @Autowired
    public KafkaProducerController(KafKaProducerService producerService)
    {
        this.producerService = producerService;
    }

    //@PostMapping(value = "/publish")
    @RequestMapping(value = "/publish")
    public void sendMessageToKafkaTopic(@RequestParam("message") String message)
    {
        this.producerService.sendMessage(message);
    }
    @RequestMapping("/hello")
    @ResponseBody
    public String hello(){
        return "hello";
    }
}