//package com.mycompany.app;
package com.mapr.examples;
import org.apache.commons.mail.*;
import java.util.*;

public class hadoop_reader {

    static String email_receiver;
    static String file_name;
    static String file_path;
    static Email email;
    static Double threshold;

    public static void test_email() throws EmailException{
        email.setSubject("Hi this is testing email only");
        email.setMsg("Hello there, testing to send email from GMail");
        email.addTo(email_receiver);
        email.send();
        System.out.println("Email to" +email_receiver + " has been sent");
        return;
    }

    public static void send_process_warning(String content) throws EmailException{
        email.setSubject("[Warning]process overload");
        email.setMsg("Hello there"+ content);

        email.addTo(email_receiver);
        email.send();
        System.out.println("Email to " +email_receiver + " has been sent");
        return;
    }


    public static String machine_monitor(List <String> table){
        String result="";
        for(int i=0; i<table.size(); i++)
        {
            String [] record = table.get(i).split(",");
            String key = record[0];
            String cpu_info= record[1];
            String mem_indo= record[2];

            // split the cpu info by " "
            // split the mem info by " "

            // find spikes in a machine
        }
        return result;
    }

    public String process_monitor(List<String> table, String mode){
        String result="Inappropriate" + mode + " set:";

        HashMap<String, process_level_record> process_map = new HashMap<String, process_level_record>();

        // first, iterate through the whole table, put records in the hash map
        for(int i=0; i<table.size(); i++)
        {
            String [] record = table.get(i).split(",");
            String key = record[0];
            process_level_record r = new process_level_record(record);
            process_map.put(key,r);
        }

        //System.out.println(process_map.size());

        // second, iterate through the hash map. for each overload state, find their solution
        for (Map.Entry<String, process_level_record> entry : process_map.entrySet()) {
            String key = entry.getKey();
            process_level_record record = entry.getValue();
            if(record.overload(threshold))
            {
                // stop searching as soon as there is a record that meet the condition
                List <process_level_record> local_solution = this.solve(record,process_map );
                if(local_solution != null)
                    result = result +"\n" + this.find_solution(local_solution, mode);
            }
        }

        // after the loop, result is a string of inappropriate user sets
        return result;
    }

    // find the subset of the key that de-overload the machine
    // input: the overloading record and the hashmap
    // output: a two-record list, first is the target, second is the solution
    // the output may be null, which means this overloading record cannot be solved
    public List<process_level_record> solve(process_level_record target, HashMap<String, process_level_record> process_map) {
        List <process_level_record> solution = new Vector<process_level_record>();
        solution.add(target);
        boolean has_solution=false;

        // iterate over the hashmap, if there is a subset of the target, check if it deoverloads the target
        for (Map.Entry<String, process_level_record> entry : process_map.entrySet()) {
            String key = entry.getKey();
            process_level_record record = entry.getValue();
            if(target.has_subset(key) && record.deoverload(this.threshold))
            {
                // stop searching as soon as there is a record that meet the condition
                solution.add(record);
                has_solution = true;
                break;
            }
        }
        if(has_solution == false)
            return null;
        else
            return solution;
    }

    // this function should be called after calling solve()
    // construct the result string by the subtracting the two combos
    // if there is no solution for the problem, return a failure log
    public String find_solution(List<process_level_record> solution, String mode) {
        String result = "";
        if(solution == null)
            return "";

        // solution[0] is the problem we want to solve
        // solution[1] is the solution to the problem, assume there is only one solution to the question
        process_level_record problem = solution.get(0);
        process_level_record answer = solution.get(1);

        // get key from the two records
        String overloading_combo = problem.key;
        String not_overloading_combo = answer.key;

        // construct reason string
        List <String> reason_list = this.subtract(overloading_combo, not_overloading_combo);
        String reason_string=reason_list.get(0);
        for(int i=1; i< reason_list.size(); i++)
        {
            reason_string += "," + reason_list.get(i);
        }

        // construct problem string and machine name
        String []problem_list = overloading_combo.split("\\+",2);
        String machine_name = problem_list[0];
        String problem_string = problem_list[1].replace("\\+", ",");

        // generate the last result by reason string and problem string
        result = mode+ " set [" + reason_string + "] overloads machine "+ machine_name+ " [" + problem_string +"]";
        return result;
    }

    // subtract the overload state by the not overload state
    // input: two strings, the first is the overloading key, second is the not overloading key
    public static List <String> subtract(String overload, String not_overload){
        String [] overload_array = overload.split("\\+");
        String [] not_overload_array = not_overload.split("\\+");
        List<String> overload_list = new LinkedList<String>(Arrays.asList(overload_array));
        List<String> not_overload_list = new LinkedList<String>(Arrays.asList(not_overload_array));

        overload_list.removeAll(not_overload_list);
        return overload_list;
    }


    public String read_hadoop_result(String option) {
        // read a txt file that hadoop mapreduce generates
        // we assumes that the task succeed and the file exists

        String result = "";
        String cat_command = "hadoop fs -cat "+file_path+"/"+file_name+"/"+"part-r-00000";
        command_line cmd = new command_line(cat_command, false);
        List<String> table;

        table = cmd.run();

        switch(option){
            case "machine": result = machine_monitor(table);break;
            case "process": result = process_monitor(table,"process");break;
            case "user": result = process_monitor(table,"user");break;
            default: System.out.println("Don't know how to monitor "+option);break;
        }

        return result;
    }



    public hadoop_reader(String file_path, String file_name,String email_receiver)throws EmailException{
        this.email_receiver = email_receiver;
        this.file_name = file_name;
        this.file_path = file_path;
        email = new SimpleEmail();
        email.setSmtpPort(587);
        email.setHostName("smtp.gmail.com");
        threshold = 60.0;

        // login my gmail account, need to provide password
        email.setAuthentication("penghao19930509@gmail.com", "110120119");
        email.setStartTLSEnabled(true);
        email.setFrom("penghao19930509@gmail.com", "Hao Peng");
    }

    public static void main(String[] args) throws EmailException{
        // usage: [monitor types] can be machine, process, user
        // usage: [mode] can be email, print
        // file path, argument 2: file name, argument 3: email address
        if(args.length<3)
        {
            System.out.println("Usage: [file name][monitor type][mode]");
            return;
        }
        String file_name = args[0];
        String monitor_type = args[1];
        String mode = args[2];

        hadoop_reader reader = new hadoop_reader("/user/hadoop-output",file_name,"466176406@qq.com");
        String warning_content= reader.read_hadoop_result(monitor_type);
        if(mode.equals("email"))
            reader.send_process_warning(warning_content);
        else
            System.out.println(warning_content);
        return;
    }
}
