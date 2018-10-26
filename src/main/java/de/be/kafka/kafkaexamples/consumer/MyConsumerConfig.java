package de.be.kafka.kafkaexamples.consumer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class MyConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        //mandatory properties
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        //non-mandatory
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, "myConsumerGroup1");//this would put all the consumers in the same group, but consumer group could be ( and it is) defined directly on the listener methods
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//if there are already messages in the topic, and there is a consumer with new consumer group accessing, all messages will be processed from the beginning of time

        //we can combine this two properties to reduce a load on both consumer and the broker when there is not too much activity and when we don't care about immediately processing new messages
        //message gets fetched only if one of this two requirements is fulfilled
        //props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,70); //This way, consumer doesn't fetch a messages until their total size is more than given value (in bytes)
        //props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 15000);//Until tih timeout passes consumer doesn't fetches messages

        //again two related properties
        //if broker doesn't get a heartbeat from consumer in this time interval, broker will remove consumer and perform rebalance to elect another consumer for that partition(s)
        //extending this is useful if consumer takes longer to perform poll operation or garbage collection. On another hand extending it could cause longer to detect consumer failures
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);//default is 10sec. it has to be between group.min.session.timeout.ms=6s and group.max.session.timeout.ms=30s
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2000); //how often consumer will send heartbeats. This value must be (logically) lower then session.timeout.ms and possibly 1/3 of it's value


        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5); //maximal number of records returned in one poll() call

        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        factory.setConcurrency(3);// if we set this value, every listener method will have three threads. But if there is just one listener method, and topic has 2 partitions, 1 thread will be idle
        //if there are 2 listener methods, this setting will create 6 threads, but since there are only 2 partitions, 4 threads will be idle
        return factory;
    }

}