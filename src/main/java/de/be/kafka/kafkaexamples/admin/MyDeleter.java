package de.be.kafka.kafkaexamples.admin;

import de.be.kafka.kafkaexamples.consumer.MyConsumerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class MyDeleter {

    @Value("${app.topic.name}")
    private String topicName;

    private AdminClient kafkaAdminClient;

    //reuses consumer configuration
    public MyDeleter(MyConsumerConfig myConsumerConfig) {
        kafkaAdminClient = AdminClient.create(myConsumerConfig.consumerConfigs());
    }

    // This operation is supported by brokers with version 0.11.0.0 or higher
    // Deletes all te messages from given partition having offset equal or lower than given
    // once you delete messages, the offsets of non-deleted messages doesn't change
    // if you provide unexisting offset no messages will be deleted
    public void deleteMessages(int partitionIndex, int allMessagesBeforeOffset) {
        TopicPartition topicPartition = new TopicPartition(topicName, partitionIndex);
        Map<TopicPartition, RecordsToDelete> deleteMap = new HashMap<>();
        deleteMap.put(topicPartition, RecordsToDelete.beforeOffset(allMessagesBeforeOffset));
        kafkaAdminClient.deleteRecords(deleteMap);
    }

}
