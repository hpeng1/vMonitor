package com.mapr.examples;

/**
 * Created by pengh on 8/8/16.
 */
class process_level_record{
    String key;
    double max_cpu;
    double average_cpu;
    double max_memory;
    double average_memory;
    int count;

    process_level_record(String [] record){
        key = record[0];
        String cpu_info_raw= record[1];
        String mem_info_raw= record[2];

        // split the cpu and mem info by " "
        String []cpu_info = cpu_info_raw.split(" ");
        String []mem_info = mem_info_raw.split(" ");

        max_cpu = Double.parseDouble(cpu_info[1]);
        average_cpu = Double.parseDouble(cpu_info[3]);
        count = Integer.parseInt(cpu_info[5]);
        max_memory = Double.parseDouble(mem_info[1]);
        average_memory = Double.parseDouble(mem_info[3]);
    }

    boolean overload(double threshold){

        // only comparing the average cpu and the threshold
        // true: this record overloads the machine
        if (average_cpu > threshold)
            return true;
        else
            return false;
    }

    boolean deoverload(double threshold){

        // only comparing the average cpu and the threshold
        // true: this record deoverlods the machine
        if(average_cpu < threshold)
            return true;
        else
            return false;
    }

    boolean has_subset (String other_key){
        String my_key = this.key;
        String []other_set = other_key.split("\\+");
        String []my_set = my_key.split("\\+");

        int i = 0;
        int j = 0;
        for (i=0; i<other_set.length; i++)
        {
            for (j = 0; j<my_set.length; j++)
            {
                if(other_set[i].equals( my_set[j] ))
                    break;
            }

        /* If the above inner loop was not broken at all then
           arr2[i] is not present in arr1[] */
            if (j == my_set.length)
                return false;
        }

    /* If we reach here then all elements of arr2[]
      are present in arr1[] */
        //return 1;
        return true;
    }

}
