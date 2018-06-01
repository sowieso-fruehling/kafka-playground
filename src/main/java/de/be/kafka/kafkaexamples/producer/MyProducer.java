package de.be.kafka.kafkaexamples.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;

@Slf4j
@RequiredArgsConstructor
@Service
public class MyProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.topic.name}")
    private String topicName;

    //message s sent asynchronously but we won't know if message is delivered successfully
    public void fireAndForgetWithKafkaMessage(String key, String content) {
        Message<String> message = createKafkaMessage(key, content);
        log.info("sending message='{}' to topic='{}'", message.getPayload(), topicName);
        kafkaTemplate.send(message);//the message will be placed in a buffer and will be sent to the broker in a separate thread
    }

    //One more way of producing messages
    public void fireAndForgetWithProducerRecord(String key, String content) {
        //there is ProducerRecord constructor accepting headers also
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, content);
        log.info("sending message='{}' to topic='{}'", record, topicName);
        kafkaTemplate.send(record);
    }


    //    simpler but less powerfull way of producing a message
    public void fireAndForgetString(String content) {
        log.info("sending message='{}' to topic='{}'", content, topicName);
        kafkaTemplate.send(topicName, content);
    }


    //synchronous sending. get() method will throw an exception if record is not sent successfully
    //in some cases producer (depending how configured) will retry before throwing an exception
    //but this approach is logiccally inefficient
    public void sendSynchronously(String key, String content) {
        Message<String> message = createKafkaMessage(key, content);
        log.info("sending message='{}' to topic='{}'", message.getPayload(), topicName);
        try {
            SendResult sendResult = kafkaTemplate.send(message).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    //kafka is highly available service and most of the messages will be successfully sent
    //but if we want to have asynchronous producers and to still be able to be informed about errors, we can register a callback
    public void sendAsynchronously(String key, String content) {
        Message<String> message = createKafkaMessage(key, content);
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                //we shouldn't log unnecessarily as it considerably slows down our producer
                log.info("successfully sent message='{}' with offset={}", message, result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("unable to send message='{}'", message, ex);
            }
        });
    }

    private Message<String> createKafkaMessage(String key, String content) {
        return MessageBuilder
                .withPayload(content)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .setHeader(KafkaHeaders.MESSAGE_KEY, key) // kafka guaranties that all the messages with the same key are going to same topic's partition, unless we specify partition manually
                //.setHeader(KafkaHeaders.PARTITION_ID, 0) // we can even manually choose a partition
                .build();
    }

}
