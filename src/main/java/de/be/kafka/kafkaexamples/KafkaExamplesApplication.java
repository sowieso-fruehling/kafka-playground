package de.be.kafka.kafkaexamples;

import de.be.kafka.kafkaexamples.admin.MyDeleter;
import de.be.kafka.kafkaexamples.producer.MyProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class KafkaExamplesApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaExamplesApplication.class, args);
    }

    private final MyProducer producer;

    private final MyDeleter myDeleter;

    @Override
    public void run(String... args){

        int counter=0;
        String messageKey="44";
        String messageValue="John";
        //produce 4 messages
        while (++counter<5) {
            producer.sendAsynchronously(messageKey, messageValue);
        }

//        int partitionToDeleteFrom=6;
//        int beforeOffset=3;
//        myDeleter.deleteMessages(partitionToDeleteFrom, beforeOffset);

    }
}
