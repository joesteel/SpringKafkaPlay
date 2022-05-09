package com.example.kafkaAmitTest;


import org.springframework.web.bind.annotation.*;

@RestController
public class HelloWorldController {

    private kafkaBasicStringRecordProducer myProducer;
    private static KafkaConsumerThread kafkaConsumerThread;

    public HelloWorldController(){
        myProducer = new kafkaBasicStringRecordProducer();
        kafkaConsumerThread = new KafkaConsumerThread();
        kafkaConsumerThread.start();
    }

    @RequestMapping("/")
    public String sendBasicMessagesToKafka(){
        myProducer.sendMessages(10);
        return "Sending a bunch of messages to kafka\n";
    }

    @RequestMapping(value="/{name}", method = RequestMethod.GET)
    public String getForName(@PathVariable String name) {
        myProducer.sendMessages(name);
        return String.format("Hello %s you crazy son of a bitch \n", name);
    }

    @RequestMapping(value="/person/{name}", method=RequestMethod.GET)
    public String findOwner(@PathVariable("name") String name) {
        myProducer.sendMessages(name);
        return String.format("it seems you were trying to get %s\n", name);
    }

    public class KafkaConsumerThread extends Thread {
        public void run(){
            System.out.println("KafkaConsumerThread started");
            KafkaBasicStringRecordConsumer myConsumer = new KafkaBasicStringRecordConsumer();
            myConsumer.listenForMessages();
        }
    }

}
