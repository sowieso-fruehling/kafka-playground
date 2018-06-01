package de.be.kafka.kafkaexamples.producer;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class MyProducerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.topic.name}")
    private String topicName;

    @Value("${app.topic.partitions}")
    private int numberOfPartitions;

    @Value("${app.topic.replicationFactor}")
    private short replicationFactor;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        //A Kafka producer has three mandatory properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);//comma separated list of brokers. Client is using just one to set up initial connection so stating only one broker is sufficient but if that one broker is down, connection to kafka will be impossible
        //Kafka accepts only byte arrays
        //Producers uses following classes to serialize key and the message respectively, to byte array
        //any class that implements the org.apache.kafka.common.serialization.Serializer interface.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //non-mandatory properties, more details https://kafka.apache.org/documentation/#producerconfigs

        //props.put(ProducerConfig.ACKS_CONFIG, 0); //this means that producer will not wait for the acknowledgement from kafka at all (not safe). Default is 1, meaning leader informs a producer weather the message is successfully received
        //props.put(ProducerConfig.RETRIES_CONFIG, 3); // if we want producer to retry failed messages on TRANSIENT errors. By default it's not enabled because it can affect ordering of the messages. If we want to be able to retry and still to preserve the order we should set property max.in.flight.requests.per.connection=1, so not another batch is produced until the first one is in the process of retrying. But this setting is severely limiting producer's throughput
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // doubling the batch size. Default is 16384. Producer sends messages as batches thus improving performance. The producer will send half-full batches and even batches with just a single message in them. Therefore, setting the batch size too large will not cause delays in sending messages; it will just use more memory for the batches.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 2000); //maximum time (in ms) to wait for batch to get filled. If this time has passed, even if batch is not full messages will be sent. Default value is 0
        //props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000); //for how long to wait for the response from kafka on each request (on each batch sent)

        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); //The total bytes of memory the producer can use to buffer records waiting to be sent to the server. If records are sent faster than they can be delivered to the server the producer will block for max.block.ms after which it will throw an exception.

        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);// how long consumer will wait for kafka broker to respond to a request. It's useful if kafka is under heavy load

        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }


    //with KafkaAdmin we can create topics programmatically
    //even without this a new topic would be crated in kafka, just the number of partitions and replication factor will be default ones
    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topic1() {
        return new NewTopic(topicName, numberOfPartitions, replicationFactor);
    }
}
