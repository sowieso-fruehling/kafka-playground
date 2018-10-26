package de.be.kafka.kafkaexamples.producer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class MyProducerTest {

    private static final String KAFKA_FILTERS_TOPIC = "mytopic"; //has to be the same as the one from application.yml file

    //for this to work, in our configuration, we need to define bootstrap-servers as ${spring.embedded.kafka.brokers}
    @ClassRule
    public static final KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1, true, KAFKA_FILTERS_TOPIC);

    private Consumer<String, String> consumer;

    private ConsumerRecords<String, String> replies;

    private boolean testEnvironmentSetUpAlready = false;

    @Autowired
    private MyProducer myProducer;

    @Before
    public void setUp() {

        //run this only once
        if (testEnvironmentSetUpAlready)
            return;

        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps("consumerGroupName", "true", kafkaEmbedded);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // create a Kafka consumer factory
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProperties);

        consumer = consumerFactory.createConsumer();

        testEnvironmentSetUpAlready = true;
    }

    @Before
    public void subscribeConsumerToATopic() throws Exception {
        kafkaEmbedded.consumeFromAnEmbeddedTopic(consumer, KAFKA_FILTERS_TOPIC);
    }

    @After
    public void closeConsumer() {
        consumer.close();
    }


    @Test
    public void thatOneMessageIsSuccessfullySent() {

        String messageKey = "44";
        String messageValue = "John";

        myProducer.sendAsynchronously(messageKey, messageValue);

        replies = KafkaTestUtils.getRecords(consumer);

        assertEquals(1, replies.count());

        assertEquals(replies.iterator().next().key(), messageKey);
    }
}
