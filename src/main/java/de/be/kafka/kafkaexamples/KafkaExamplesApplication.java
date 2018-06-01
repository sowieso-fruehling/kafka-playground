package de.be.kafka.kafkaexamples;

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

    @Override
    public void run(String... args) throws Exception {
        log.info("App started");

//        long t = System.currentTimeMillis();
//        long end = t + 10000;
//
//        //produce for 10 seconds
//        while (System.currentTimeMillis() < end) {
//            producer.sendAsynchronously("2", "John");
//        }

        int counter=0;
        //produce 4 messages
        while (++counter<5) {
            producer.sendAsynchronously("2", "John");
        }

        System.out.println("app closed");
    }
}
