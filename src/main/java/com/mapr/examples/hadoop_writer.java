package com.mapr.examples;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by pengh on 6/27/16.
 * this class reads the output from consumer and write then into a file for hadoop usage
 */
public class hadoop_writer {

    private String file_name;
    private String content;

    // initiate the class by passing a file name
    public hadoop_writer(String file_name, String content){
        this.file_name = file_name;
        this.content = content;
    }

    // write the content to the file
    public void write(){
        try {
            File log_file =new File("./hadoop-files/",file_name+".txt");
            FileWriter fileWritter = new FileWriter(log_file,true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);

            bufferWritter.write(content);
            bufferWritter.newLine();
            bufferWritter.close();

        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args){

        // the main function just write a line of status to a file called test.txt
        // arguments: filename, message
        // message format: [vm name]|[time]|[cpu usage]\[total process number]\[active number]\[sleeping number]
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        hadoop_writer h = new hadoop_writer("test","VM14|"+timeStamp+"|5.93|133|4|129");
        h.write();
        return;
    }
}
