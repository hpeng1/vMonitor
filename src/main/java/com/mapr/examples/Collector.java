package com.mapr.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import java.net.InetAddress;
import java.net.UnknownHostException;
/**
 * Created by Hao Peng on 6/15/16.
 */
public class Collector {

    public static List<String> hardware_metrics(){

        // this function returns a list of hardware properties

        List<String> metrics  = new Vector<String>();

        int processors = Runtime.getRuntime().availableProcessors();
        long free_memory = Runtime.getRuntime().freeMemory();
        long total_memory = Runtime.getRuntime().totalMemory();

        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        InetAddress ip;
        String hostname="unknown";
        try{
            ip =InetAddress.getLocalHost();
            hostname = ip.getHostName();
        }
        catch (UnknownHostException e){
            e.printStackTrace();
        }

        metrics.add("{\"vm_name\":"+"\""+hostname+"\""+",");
        metrics.add("\"time\":"+"\""+timeStamp+"\""+",");
        metrics.add("\"processors\":" + processors+",");
        metrics.add("\"memory\":" + free_memory+",");
        metrics.add("\"JVM memory\":" + total_memory+"}");

        //System.out.println(metrics);

        return metrics;
    }

    public static List<String> software_metrics(){

        // this function returns a list of software properties

        List<String> metrics  = new Vector<String>();

        String java_version = System.getProperty("java.version");
        String jvm_version = System.getProperty("java.vm.name") + " " + System.getProperty("java.vm.version");
        String os = System.getProperty("os.name");
        String os_arch = System.getProperty("os.arch");

        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        InetAddress ip;
        String hostname="unknown";
        try{
            ip =InetAddress.getLocalHost();
            hostname = ip.getHostName();
        }
        catch (UnknownHostException e){
            e.printStackTrace();
        }

        metrics.add("{\"vm_name\":"+"\""+hostname+"\""+",");
        metrics.add("\"time\":"+"\""+timeStamp+"\""+",");
        metrics.add("\"os\":"+ "\""+os+ "\",");
        metrics.add("\"architecture\":"+ "\""+os_arch+ "\",");
        metrics.add("\"java\":"+ "\""+java_version+ "\",");
        metrics.add("\"jvm\":"+ "\""+ jvm_version+"\"}");

        //System.out.println(metrics);

        return metrics;
    }

    private static List<String> cut(List<String> metric){

        String tasks="";
        String running="";
        String sleeping="";
        String cpu_usage="";

        String os_arch = System.getProperty("os.name");
        int cpu_line_number=0;
        int cpu_position=0;
        int tasks_line_number=0;
	int table_line_number=0;
        int tasks_position,running_position,sleeping_position=0;
        List<String> brief_metric = new Vector<String>();

        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        InetAddress ip;
        String hostname="unknown";
        try{
            ip =InetAddress.getLocalHost();
            hostname = ip.getHostName();
        }
        catch (UnknownHostException e){
            e.printStackTrace();
        }

        // find where the info is located
        for(int i=0; i<metric.size(); i++)
        {
            // first format is linux system, second format is Mac system
            if(metric.get(i).startsWith("%Cpu(s):") || metric.get(i).startsWith("CPU usage:"))
                cpu_line_number=i;
            if(metric.get(i).startsWith("Tasks:") || metric.get(i).startsWith("Processes:"))
                tasks_line_number=i;
	    if(metric.get(i).contains("PID") &&  metric.get(i).contains("USER"))
		table_line_number = i;

        }
        //System.out.println("cpu info is in line "+cpu_line_number);
        //System.out.println("tasks info is in line "+tasks_line_number);

        // fetch the cpu and tasks information
        String raw_string = metric.get(cpu_line_number).replace("%","").replace(" ","");
        String[] str_cpu = raw_string.split(",");
        String[] str_tasks = metric.get(tasks_line_number).replace(" ","").split(",");

        if (os_arch.contains("Mac")){
		// for mac user
            cpu_usage = str_cpu[2];
            tasks = str_tasks[1];
            running=str_tasks[3];
            sleeping = str_tasks[7];
        }
        else{
		
		// for linux user
            cpu_usage = str_cpu[0].replaceAll("[^\\d.]", "");
            tasks = str_tasks[0].replaceAll("[^\\d.]", "");
            running=str_tasks[1].replaceAll("[^\\d.]", "");
            sleeping = str_tasks[2].replaceAll("[^\\d.]", "");
        }

        /*
        String vm_name = message.get("vm_name").asText();
        String time = message.get("time").asText();
        String cpu_usage = message.get("cpu_usage").asText();
        String process_number = message.get("process_number").asText();
        String active_number = message.get("active_number").asText();
        String sleep_number = message.get("sleep_number").asText();
        */

 	    brief_metric.add("{\"vm_name\":"+"\""+hostname+"\""+",");
        brief_metric.add("\"time\":"+"\""+timeStamp+"\""+",");
        brief_metric.add("\"cpu_usage\":"+"\""+cpu_usage+"\""+",");
        brief_metric.add("\"process_number\":"+"\""+tasks+"\""+",");
        brief_metric.add("\"active_number\":"+"\""+running+"\""+",");
        brief_metric.add("\"sleep_number\":"+"\""+sleeping+"\""+",");

	String gaint_string = "";
	// for the timebeing table is a gaint string
	
	for(int i=table_line_number+1; i<metric.size(); i++)
	{
		gaint_string = gaint_string+metric.get(i)+"|";
	}
	brief_metric.add("\"table\":"+"\""+gaint_string+"\""+"}");

	System.out.println(brief_metric);
	//System.out.println("table starts with: " + table_line_number);

        return brief_metric;
    }

    public static List<String> process_metrics() {

        //this function returns a list of current processes and their status
        //this metric fresh every 1 second, call it multiple times to see the system's activeness

        List<String> metrics;
        List<String> brief_metircs;
        command_line top_cmd;
        String os_arch = System.getProperty("os.name");

        // determine the operating system
        // command line for system status:
        // "top -l 1" for mac
        // "top -bn1" for linux

        if (os_arch.contains("Mac"))
            top_cmd = new command_line("top -l 1", false);
        else
            top_cmd = new command_line("top -bn1", false);

        metrics = top_cmd.run();
        brief_metircs = cut(metrics);
        // need to process metrics

        return brief_metircs;
    }

    public static ProducerRecord<String, String> pack_for_producer(String type, List<String> metrics)throws IOException{

        // this function truns a list of strings into a message object
        // a message object can be used by the producer
        ProducerRecord rcd;
        String topic;
        String content = "";

        // determine the topic
        switch (type){
            case "hardware": topic="hardware";break;
            case "software": topic="software";break;
            case "process": topic="process";break;
            default: topic="unknown_topic";
        }

        // turn the list of strings into a large string
        for(int i=0; i< metrics.size(); i++) {
            content = content.concat(metrics.get(i));
            content = content.concat("\n");
        }


        rcd= new ProducerRecord<String, String>(topic,content);
        return rcd;
    }

    public static ProducerRecord report_hardware()throws IOException{
        List<String> hardware_metrics;
        hardware_metrics = hardware_metrics();
        ProducerRecord hardware_record = pack_for_producer("hardware",hardware_metrics);
        return hardware_record;
    }

    public static ProducerRecord report_software()throws IOException{
        List<String> software_metrics;
        software_metrics = software_metrics();
        ProducerRecord software_record = pack_for_producer("software",software_metrics);
        return software_record;
    }

    public static ProducerRecord report_process()throws IOException{
        List<String> process_metrics;
        process_metrics = process_metrics();
        ProducerRecord process_record = pack_for_producer("process",process_metrics);
        return process_record;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            // at least one argument to run Run.class
            throw new IllegalArgumentException("Collector must have 1 string as argument");
        }

        //ProducerRecord hardware_record = report_hardware();
        //ProducerRecord software_record = report_software();
        ProducerRecord process_record = report_process();

    }
}
