package com.mapr.examples;

import org.apache.commons.mail.EmailException;

import java.io.IOException;

/**
 * Pick whether we want to run as producer or consumer. This lets us
 * have a single executable as a build target.
 */
public class Run {
    public static void main(String[] args) throws IOException, EmailException, InterruptedException{


        switch (args[0]) {
            case "producer":
                //Producer.main(args);  // another way to invoke producer
                if (args.length < 3) {
                    throw new IllegalArgumentException("Producer must have at least 2 arguments:[type][number]");
                }
                int number = Integer.parseInt(args[2]);
                Producer p = new Producer();
                p.produce(args[1], number);
                break;
            case "consumer":
                //Consumer.main(args);   // another way to invoke consumer
                if (args.length < 2) {
                    throw new IllegalArgumentException("Consumer must have at least 1 arguments:[number]");
                }
                int num = Integer.parseInt(args[1]);
                Consumer c = new Consumer(num);
                c.process_records();
                break;
            case "collector":
                Collector.main(args);
                break;
            case "hadoop-writer":
                hadoop_writer.main(args);
                break;
            case "hadoop-reader":
                if (args.length < 2) {
                    throw new IllegalArgumentException("Consumer must have at least 1 arguments:[your email address]");
                }
                hadoop_reader email = new hadoop_reader(args[1]);
                email.test_email();
                break;
            default:
                throw new IllegalArgumentException("Don't know what to do with" + args[0]);
        }
    }
}
