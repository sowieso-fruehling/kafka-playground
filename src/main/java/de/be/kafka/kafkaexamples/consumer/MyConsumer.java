package de.be.kafka.kafkaexamples.consumer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
//so we're having 4 different consumers, two in the consumer group 1 and other two in the different groups.
// That means that for every message produced to ${app.topic.name} topic, three different consumers will be executed ( each message will be processed in three different ways)
//per message produced to topic, only one of the consumers from group1 will be executed, depending on which partition message ended, and which consumer of these two is listening to that partition.
//This doesn't mean that another consumer from group1 won't be executed ever, if topic is having more than one partition, second method will consume messages from that partition
//All this apply only if concurrency setting in MyConsumerConfig class is not changed to be bigger than 1. If it is, total number of consumer threads will be multiplied with concurrency setting value
public class MyConsumer {


    @KafkaListener(id = "simpleConsumer1",topics = "${app.topic.name}", groupId="group1")
    public void receive1(@Payload String data ) {

        log.info("- - - - - - - - - - - - - - -");
        log.info("(1) received message='{}'", data);
    }

    @KafkaListener(id = "simpleConsumer2",topics = "${app.topic.name}", groupId="group1")
    public void receive2(@Payload String data ) {

        log.info("- - - - - - - - - - - - - - -");
        log.info("(2) received message='{}'", data);
    }

    @KafkaListener(id = "withIndividualHeaders",topics = "${app.topic.name}", groupId="group2")
    public void receive(@Payload String data,
                        //all this headers have to exist. Otherwise an exception would occur
                        @Header(KafkaHeaders.OFFSET) Long offset,
                        @Header(KafkaHeaders.CONSUMER) KafkaConsumer<String, String> consumer,
                        @Header(KafkaHeaders.TIMESTAMP_TYPE) String timestampType,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
                        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String messageKey,
                        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp
    ) {

        log.info("- - - - - - - - - - - - - - -");
        log.info("received message='{}'", data);

        log.info("consumer: {}", consumer);
        log.info("topic: {}", topic);
        log.info("message key: {}", messageKey);
        log.info("partition id: {}", partitionId);
        log.info("offset: {}", offset);
        log.info("timestamp type: {}", timestampType);
        log.info("timestamp: {}", timestamp);
    }

    @KafkaListener(id = "withHeaderMap", topics = "${app.topic.name}", groupId = "group3")
    public void receive(@Payload String data,
                        @Headers MessageHeaders messageHeaders) {

        log.info("- - - - - - - - - - - - - - -");
        log.info("received message='{}'", data);
        messageHeaders.keySet().forEach(key -> {
            Object value = messageHeaders.get(key);
            log.info("{}: {}", key, value);

        });

    }
}