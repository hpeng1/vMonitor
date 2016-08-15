package com.mapr.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * This program reads messages from two topics. Messages on "fast-messages" are analyzed
 * to estimate latency (assuming clock synchronization between producer and consumer).
 * <p/>
 * Whenever a message is received on "slow-messages", the stats are dumped.
 */
public class Consumer {

    public static int timeouts = 0;
    public int max_number;
    public int number;
    public KafkaConsumer<String, String> consumer;

    public void handle_process_message(String topic, JsonNode message){
        // this function handles process message
        // convert JsonNode into formatted hadoop record
        String hadoop_record="";

        String vm_name = message.get("vm_name").asText();
        String time = message.get("time").asText();
        String cpu_usage = message.get("cpu_usage").asText();
        String process_number = message.get("process_number").asText();
        String active_number = message.get("active_number").asText();
        String sleep_number = message.get("sleep_number").asText();
        String process_table = message.get("table").asText();

        hadoop_record = vm_name+"|"+time+"|"+cpu_usage+"|"+process_number+"|"+active_number+"|"+sleep_number+"|"+process_table;

        hadoop_writer h = new hadoop_writer(topic, hadoop_record);
        h.write();
        return;
    }

    public void handle_hardware_message(String topic, JsonNode message){
        // to be implemneted
        String hadoop_record="";

        String vm_name = message.get("vm_name").asText();
        String time = message.get("time").asText();
        String processors = message.get("processors").asText();
        String memory = message.get("memory").asText();
        String JVM_memory = message.get("JVM memory").asText();

        hadoop_record = vm_name+"|"+time+"|"+processors+"|"+memory+"|"+JVM_memory;

        hadoop_writer h = new hadoop_writer(topic, hadoop_record);
        h.write();
        return;
    }

    public void handle_software_message(String topic, JsonNode message){
        // to be implemented
        String hadoop_record="";

        String vm_name = message.get("vm_name").asText();
        String time = message.get("time").asText();
        String os = message.get("os").asText();
        String architecture = message.get("architecture").asText();
        String java = message.get("java").asText();
        String jvm = message.get("jvm").asText();

        hadoop_record = vm_name+"|"+time+"|"+os+"|"+architecture+"|"+java+"|"+jvm;

        hadoop_writer h = new hadoop_writer(topic, hadoop_record);
        h.write();
        return;
    }

    public void process_records() throws IOException {

        // make a new mapper for records
        ObjectMapper mapper = new ObjectMapper();

        while (true) {
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(500);
            number = number + records.count();

            if( number >= max_number){
                System.out.println("Consumer exited after consuming "+(number - records.count())+" messages");
                break;
            }

            if (records.count() == 0 ) {
                // consumer exits after 5 timeout
                timeouts++;
                //if(timeouts>=5)
                    //break;

            } else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, String> record : records) {
                switch (record.topic()) {
                    case "fast-messages":
                        // the send time is encoded inside the message
                        JsonNode msg = mapper.readTree(record.value());
                        switch (msg.get("type").asText()) {
                            case "test":
                                break;
                            case "marker":
                                break;
                            default:
                                throw new IllegalArgumentException("Illegal message type: " + msg.get("type"));
                        }
                        break;
                    case "summary-markers":
                        break;
                    case "hardware":
                        System.out.println("handle hardware message");
                        JsonNode hardware_message = mapper.readTree(record.value());
                        handle_hardware_message(record.topic(), hardware_message);
                        break;
                    case "software":
                        System.out.println("handle software message");
                        JsonNode software_message = mapper.readTree(record.value());
                        handle_software_message(record.topic(), software_message);
                        break;
                    case "process":
                        System.out.println("handle process message");
                        JsonNode process_message = mapper.readTree(record.value());
                        handle_process_message(record.topic(), process_message);
                        break;
                    default:
                        throw new IllegalStateException("Shouldn't be possible to get message on topic: " + record.topic());
                }

            }
        }

        //System.out.println("All messages are handled");


    }

    public Consumer(int max) throws IOException {

        // initiate number , max number and the consumer class
        number = 0;
        this.max_number = max;
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            consumer = new KafkaConsumer<>(properties);
        }
        consumer.subscribe(Arrays.asList("hardware", "software", "process"));

    }


    public static void main(String[] args) throws IOException {
        // set up house-keeping
        /*
        ObjectMapper mapper = new ObjectMapper();
        Histogram stats = new Histogram(1, 10000000, 2);
        Histogram global = new Histogram(1, 10000000, 2);

        // and the consumer
        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            consumer = new KafkaConsumer<>(properties);
        }
        consumer.subscribe(Arrays.asList("fast-messages", "summary-markers", "hardware", "software", "process"));

        // consumer is set, ready to fetch records
        //noinspection InfiniteLoopStatement
        while (true) {
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, String> record : records) {
                switch (record.topic()) {
                    case "fast-messages":
                        // the send time is encoded inside the message
                        JsonNode msg = mapper.readTree(record.value());
                        switch (msg.get("type").asText()) {
                            case "test":
                                long latency = (long) ((System.nanoTime() * 1e-9 - msg.get("t").asDouble()) * 1000);
                                stats.recordValue(latency);
                                global.recordValue(latency);
                                break;
                            case "marker":
                                // whenever we get a marker message, we should dump out the stats
                                // note that the number of fast messages won't necessarily be quite constant
                                System.out.printf("%d messages received in period, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
                                        stats.getTotalCount(),
                                        stats.getValueAtPercentile(0), stats.getValueAtPercentile(100),
                                        stats.getMean(), stats.getValueAtPercentile(99));
                                System.out.printf("%d messages received overall, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
                                        global.getTotalCount(),
                                        global.getValueAtPercentile(0), global.getValueAtPercentile(100),
                                        global.getMean(), global.getValueAtPercentile(99));

                                stats.reset();
                                break;
                            default:
                                throw new IllegalArgumentException("Illegal message type: " + msg.get("type"));
                        }
                        break;
                    case "summary-markers":
                        break;
                    default:
                        throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
                }
            }
        }
        */
    }

}
