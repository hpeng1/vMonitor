//package com.test.wordcount; // for hadoop
package com.mapr.examples;  // for local IDE

import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

    // we can have different ways to generate the key
    // option 1: vm_name and all running processes' command
    // option 2: vm_name and all users
    // option 3 and others: vm_name only

    // we can choose if we want duplicate command/user name or not
    // 0 is no, no duplicates. 1 is yes.
    public static int duplicate=0;

    // this function returns the running processes and their information
    public static Vector <String> get_running_processes(String raw_table){
        Vector<String> processes = new Vector<String>();
        String [] process_table = raw_table.split(",");
        for(int i=0; i<process_table.length; i++)
        {
            String raw_record = process_table[i];
            String [] field = raw_record.trim().split(" ");
            String cpu_usage= field[8];
            String state = field[7];

            // [pid],[user],[command],[cpu],[memory],[state]
            if(state.equals("R") || Double.parseDouble(cpu_usage)>0)
                processes.add(field[0]+","+field[1]+","+field[11]+","+field[8]+","+field[9]+","+field[7]);

        }
        return processes;
    }

    public static String get_running_command(Vector <String> table){
        String all_running_command = "";
        Vector <String> all_commands = new Vector<>();
        for(int i=0; i<table.size(); i++)
        {
            String raw_record = table.get(i);
            String [] field = raw_record.split(",");
            String command_name = field[2];

            if(duplicate!=0 || !all_commands.contains(command_name))
            {
                all_running_command = all_running_command+"+"+command_name.replace("\t", "").replace(" ","");
                all_commands.add(command_name.replace("\t", ""));
            }

        }
        return all_running_command;
    }

    public static String get_running_user(Vector <String> table){
        String all_running_user = "";
        Vector <String> all_users = new Vector<>();
        for(int i=0; i<table.size(); i++)
        {
            String raw_record = table.get(i);
            String [] field = raw_record.split(",");
            String user_name = field[1];

            if(duplicate!=0 || !all_users.contains(user_name))
            {
                all_running_user = all_running_user+"+"+user_name.replace("\t", "").replace(" ","");
                all_users.add(user_name.replace("\t", ""));
            }


        }
        return all_running_user;
    }

    public static Text generate_key(String vm_name, Vector <String> table, String option){
        Text key;
        // three ways to generate keys: process name, user name, machine name

        switch (option){
            case "process":key = new Text(vm_name+":"+get_running_command(table).replace("\t", "").trim());break;
            case "user":key = new Text(vm_name+":"+get_running_user(table).replace("\t", "").trim());break;
            default: key = new Text(vm_name);break;
        }
        return key;
    }

    // this function sums up the total cpu of a given table
    public static double sum_up_cpu(Vector <String> running_table){
        double sum = 0.0;
        for(int i=0; i<running_table.size(); i++)
        {
            String raw_record = running_table.get(i);
            String [] field = raw_record.split(",");
            double cpu_usage = Double.parseDouble(field[3]);
            sum = sum + cpu_usage;
        }
        return sum;
    }

    // this function sums up the total memory of a given table
    public static double sum_up_memory(Vector <String> running_table){
        double sum = 0.0;
        for(int i=0; i<running_table.size(); i++)
        {
            String raw_record = running_table.get(i);
            String [] field = raw_record.split(",");
            double mem_usage = Double.parseDouble(field[4]);
            sum = sum + mem_usage;
        }
        return sum;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // map function for counting max
            String[] str = value.toString().split("\\|");
            String vm_name=str[0];
            String time=str[1];
            String cpu=str[2];
            String process_number = str[3];
            String active_number = str[4];
            String sleep_number = str[5];
            String process_table = str[6];
            Configuration conf = context.getConfiguration();
            String option = conf.get("option");

            Text info_chunk = new Text(time+"|"+cpu+"|"+process_number+"|"+active_number+"|"+sleep_number+"|"+process_table);
            Vector <String> running_processes = get_running_processes(process_table);
            Text mapping_key = generate_key(vm_name, running_processes, option);

            context.write(mapping_key, info_chunk);
            //context.write(new Text(vm_name), info_chunk);
        }
    }

    public static class Reduce extends Reducer<Text,Text , Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] info;
            int count =0;
            double cpu_usage = 0.0;
            double memory_usage = 0.0;
            Configuration conf = context.getConfiguration();
            String option = conf.get("option");

            List <Double> cpu_list = new Vector<Double>();
            List <Double> mem_list = new Vector<Double>();
            List <String> date_list = new Vector<String>();

            // iterate through the info chunks
            for (Text val : values) {
                count++;
                info = val.toString().split("\\|");
                String time = info[0];
                String raw_table = info[5];
                Vector <String> table = get_running_processes(raw_table);
                cpu_usage = sum_up_cpu(table);
                memory_usage = sum_up_memory(table);
                cpu_list.add(cpu_usage);
                mem_list.add(memory_usage);
                date_list.add(time);
            }


            // option: machine level monitoring or process level monitoring
            if(option.equals("machine"))
            {
                // initiate a machine level statics class
                machine_level_Statics m = new machine_level_Statics(key.toString(),date_list,cpu_list,mem_list);
                String machine_cpu_summary = m.get_cpu_summary();
                String machine_mem_summary = m.get_mem_summary();
                context.write(key, new Text(","+machine_cpu_summary+","+machine_mem_summary));
            }
            else {
                // initiate a process level statics class
                process_level_Statics cpu_stat = new process_level_Statics(toArray(cpu_list));
                process_level_Statics mem_stat = new process_level_Statics(toArray(mem_list));

                double cpu_average = cpu_stat.getMean();
                double memory_average = mem_stat.getMean();

                double max_cpu = cpu_stat.getMax();
                double max_memory = mem_stat.getMax();

                double cpu_stdDev = cpu_stat.getStdDev();
                double mem_stdDev = mem_stat.getStdDev();

                double cpu_median = cpu_stat.getMedian();
                double mem_median = mem_stat.getMedian();

                String cpu_summary = "max_cpu: " + max_cpu + " " + "average: " + cpu_average + " " + "count: " + count + " median: " + cpu_median + " cpu_stdDev: " + cpu_stdDev;
                String memory_summary = "max_memory: " + max_memory + " " + "average: " + memory_average + " " + "count: " + count + " median: " + mem_median + " mem_stdDev: " + mem_stdDev;
                context.write(key, new Text("," + cpu_summary + "," + memory_summary));
            }
        }
    }

    static double[] toArray (List <Double> doubles){
        double[] target = new double[doubles.size()];
        for (int i = 0; i < target.length; i++) {
            target[i] = doubles.get(i);                // java 1.5+ style (outboxing)
        }
        return target;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String option = args[2];
        conf.set("option", option);

        Job job = new Job(conf, "wordcount");

        job.setJarByClass(WordCount.class);

        // output format <key, value>, <text, double>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set mapper and reducer class
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // input and output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // block until task complete
        job.waitForCompletion(true);
    }

}