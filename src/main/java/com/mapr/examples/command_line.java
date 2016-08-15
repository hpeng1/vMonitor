package com.mapr.examples;


import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by pengh on 6/16/16.
 */
public class command_line {

    private String cmd;
    private boolean print_to_console;


    private static Process execute_command(String cmd){
        Process p;
        try {
            p = Runtime.getRuntime().exec(new String[]{"bash", "-c", cmd});
            String.format("command line \"%s\" is created:", cmd);
            return p;
        }
        catch (Exception ex) {
            String.format("command line \"%s\" cannot be created:", cmd);
            ex.printStackTrace();
            return null;
        }
    }

    private List<String> see_execution_result(Process p){
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()) );
        String line="";
        File log_file = null;
        FileWriter fileWritter = null;
        BufferedWriter bufferWritter = null;
        List<String> cmd_log = new Vector<String>();

        if(!print_to_console)
        {
            try {
                log_file =new File("cmd_logs.txt");
                fileWritter = new FileWriter(log_file.getName(),true);
                bufferWritter = new BufferedWriter(fileWritter);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        try {
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            dateFormat.format(date);
            bufferWritter.write("[INFO] Executed \"" + this.cmd+ "\" at "+ dateFormat.format(date)+":\n");
            while ((line = in.readLine()) != null) {
                cmd_log.add(line);
                if(print_to_console)
                {
                    System.out.println(line);
                }
                else
                    ;//bufferWritter.write(line+"\n");

            }
            in.close();
            if(!print_to_console)
            {
                bufferWritter.write("\n");
                bufferWritter.close();
            }

        }
        catch (Exception ex) {
            // there is error executing the command
            String.format("process has no return message" );
            ex.printStackTrace();
        }

        return cmd_log;
    }

    public command_line(String c, boolean p){
        this.cmd = c;
        this.print_to_console = p;
    }

    public List<String> run(){
        try {
            Process p = execute_command(cmd);
            List<String> cmd_log = see_execution_result(p);
            p.waitFor();
            return cmd_log;
        }
        catch (Exception ex) {
            // there is error executing the command
            String.format("command line \"%s\" cannot be executed:", cmd);
            ex.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws IOException {
        return;
    }
}
