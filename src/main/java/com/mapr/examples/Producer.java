package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class Producer {

    KafkaProducer<String, String> producer;
    Collector collector;
    int count;

    public void produce_hardware()throws IOException{
        ProducerRecord hardware_record = collector.report_hardware();
        producer.send(hardware_record);
        System.out.println("Producer sent msg number " + count++);
        return;
    }

    public void produce_software()throws IOException{
        ProducerRecord software_record = collector.report_software();
        producer.send(software_record);
        System.out.println("Producer sent msg number " + count++);
        return;
    }

    public void produce_process()throws IOException{
        ProducerRecord process_record = collector.report_process();
        producer.send(process_record);
        System.out.println("Producer sent msg number " + count++);
        return;
    }

    public Producer() throws IOException{

        // set up the producer class
        collector= new Collector();
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }
        count=0;
    }

    public void produce(String type, int number)throws IOException,InterruptedException{

        // call this function to use a Producer class
        for(int i=0; i<number; i++) {
            // sleep for 1 second
            Thread.sleep(100);
            switch (type) {
                case "hardware":
                    produce_hardware();
                    break;
                case "software":
                    produce_software();
                    break;
                case "process":
                    produce_process();
                    break;
                default:
                    System.out.println("Don't know how to produce that");
            }
        }
        System.out.println("Producer exited after sending "+ number+" messages");
        return;
    }

    public static void main(String[] args) throws IOException {

        /*
        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        try {
            for (int i = 0; i < 100000; i++) {

                // send lots of messages
                producer.send(new ProducerRecord<String, String>(
                        "fast-messages",
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));


                // every so often send to a different topic
                if (i % 1000 == 0) {
                    producer.send(new ProducerRecord<String, String>(
                            "fast-messages",
                            String.format("{\"type\":\"marker\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                    producer.send(new ProducerRecord<String, String>(
                            "summary-markers",
                            String.format("{\"type\":\"other\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                    producer.flush();

                    System.out.println("Sent msg number " + i);
                }
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

        */
    }
}
