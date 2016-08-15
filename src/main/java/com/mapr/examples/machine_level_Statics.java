//package com.mapr.examples;
package com.test.wordcount; // for hadoop
import java.util.Stack;
import java.util.List;
import java.util.Vector;


/**
 * Created by pengh on 8/11/16.
 */
public class machine_level_Statics {

    String machine_name;
    List<String> date;
    double[] cpu;
    double[] mem;
    double cpu_threshold;
    double mem_threshold;
    double idle_cpu;
    double idle_mem;

    String random_cpu_peaks_summary="";
    String random_mem_peaks_summary="";

    void set_idle_threshold(){
        // there might be more complex way to set idle thresholds in the future
        idle_cpu = 10.0;
        idle_mem = 10.0;
        return;
    }

    void set_overload_threshold(){
        // there might be more complex way to set overload thresholds in the future
        cpu_threshold = 50.0;
        mem_threshold = 40.0;
        return;
    }

    // this function returns a peak element in an array
    // complexity is log(n)
    double find_peak(List <Double> num) {
        int n = num.size();
        if(n == 1) {
            return 0;
        }

        int start = 0;
        int end = n - 1;
        int mid = 0;

        while(start <= end) {
            mid = start + (end - start) / 2;
            if((mid == 0 || num.get(mid) >= num.get(mid - 1) &&
                    (mid == n - 1 || num.get(mid) >= num.get(mid + 1)))) {
                return mid;
            }else if(mid > 0 && num.get(mid-1) > num.get(mid)) {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        return mid;
    }

    /*
    // this function returns multiple peaks in an array
    public List<Integer> find_peaks(double[] peaks) {
        if(peaks == null || peaks.length <= 1) {
            // no peaks for this array
            return null;
        }

        List<Integer> result = new Vector<Integer>();
        int top = 0;
        boolean waterFilled = false;
        Stack<Integer> stack = new Stack<Integer>();
        //Push the 0th index initially in the stack
        stack.push(0);
        for(int i = 1; i < peaks.length; i++) {
            while(peaks[i] >= peaks[stack.peek()]) { //keep popping while array element is >= array[stack.top]
                top = stack.pop();
                if(!stack.isEmpty()) {
                    if(peaks[i] > peaks[top]) {
                        //result[top] = 1; //mark the value with relevant index
                        result.add(top);
                        if(!waterFilled) {
                            waterFilled = true;
                        }
                    }
                    else {
                        stack.push(top); //push back the popped element
                        break; //To avoid infinite loop
                    }
                }
                else { //Means, the array element is less than array[stack.top]
                    break; // Ignore the popped element
                }
            }
            stack.push(i);
        }

        if(waterFilled) {
            return result;
        }
        return null;
    }
*/

    public List<Integer> find_peaks(double[] peaks){
        List<Integer> result = new Vector<Integer>();
        for(int i =1; i< peaks.length-1; i++)
        {
            if(peaks[i]>peaks[i-1] && peaks[i]>peaks[i+1])
                result.add(i);
        }
        return result;
    }
    // this function generates a report based on the peaks location
    String process_peaks(List<Integer> peaks, String type){

        String report="";
        // group the peaks and generate a list of integer list
        List<List<Integer>> peak_groups = group_peaks(peaks);
        report+= (peaks.size()+ " peaks in "+type+",");

        // iterate over the list, generate report on each list
        for(List<Integer> list: peak_groups){
            String r = generate_report(list,type);
            if(r.equals("")==false)
                report += r+"|";
        }
        return report;
    }

    String generate_report(List<Integer> peaks, String type){
        String report="";
        if(is_high_usage(peaks,type))
        {
            String start_time=date.get(peaks.get(0));
            String end_time =date.get(peaks.get(peaks.size()-1));
            report = start_time+ " to "+ end_time;
        }
        return report;
    }

    // this function checks the detailed usage
    boolean is_high_usage(List<Integer> peaks, String type){
        List<Double> usage = new Vector();

        if(peaks == null)
        {
            return false;
        }
        int start_index = peaks.get(0);
        int end_index = peaks.get(peaks.size()-1);
        double peaks_average=0.0;
        double usage_average=0.0;

        // calculate peaks_average
        double peaks_sum=0.0;
        int peaks_count=0;
        for(int i: peaks)
        {
            peaks_count++;
            if(type.equals("cpu"))
                peaks_sum+= this.cpu[i];
            else
                peaks_sum+= this.mem[i];
        }
        peaks_average = peaks_sum/peaks_count;

        // calculate usage_average
        double usage_sum=0.0;
        int usage_count=0;
        for(int i= start_index; i< end_index; i++)
        {
            usage_count++;
            if(type.equals("cpu"))
                usage_sum+= this.cpu[i];
            else
                usage_sum+= this.mem[i];
        }
        usage_average = usage_sum/usage_count;

        // this is a high usage when average usage is > 0.3 * average of peak usages
        if(usage_average >= (peaks_average*0.3) )
            return true;
        else
            return false;
    }

    // this function turns a list to a list of lists, based on how close each record is to each other
    List<List<Integer>> group_peaks( List<Integer> peak_groups){
        List<List<Integer>> result = new Vector();
        List<Integer> current_list = new Vector();
        int last_index=0;
        int current_index=0;

        // iterate over all the indexes, group the close indexes
        for(int index: peak_groups){
            current_index = index;
            // if close enough, add the index to the current list
            if(close_enough(current_index,last_index))
                current_list.add(current_index);
            else
            {
                // if not close enough
                result.add(current_list);
                current_list = new Vector();
                current_list.add(current_index);
            }
            last_index = current_index;
        }
        result.add(current_list);

        return result;
    }

    boolean close_enough(int current_index, int last_index){
        String current_time = date.get(current_index);
        String last_time = date.get(last_index);

        // date is in the format: 2016.07.17.15.29.39


        String[] time_c = current_time.split("\\.");
        String[] time_l = last_time.split("\\.");

        if(time_c == null || time_l==null)
            return true;

        int current_minute = Integer.parseInt(time_c[4]);
        int last_minute = Integer.parseInt(time_l[4]);
        int current_second = Integer.parseInt(time_c[5]);
        int last_second = Integer.parseInt(time_l[5]);

        if(time_c[0].equals( time_l[0]) && time_c[1].equals(time_l[1]) && time_c[2].equals(time_l[2]) && time_c[3].equals(time_l[3]))
        {
            // two indexes are close when they are within 20 seconds
            if((current_minute==last_minute && (current_second-last_second<=20))  || (current_minute==(last_minute+1) && (current_second+60)<(last_second+20)))
                return true;
            /*
            else if((current_minute==last_minute && (last_second-current_second<=20)) || (last_minute==(current_minute+1) && (last_second+60)<(current_second+20)))
                return true;
                */
            else
                return false;
        }
        else
            return false;

    }

    // this function convert a list of dooubles into an array of doubles
    double[] toArray (List <Double> doubles){
        double[] target = new double[doubles.size()];
        for (int i = 0; i < target.length; i++) {
            target[i] = doubles.get(i);                // java 1.5+ style (outboxing)
        }
        return target;
    }

    String get_cpu_summary(){
        // entry function to get cpu summary
        String cpu_summary = "cpu_summary:";

        // get all the indexes of peaks
        List<Integer> cpu_peaks = find_peaks(cpu);

        cpu_summary += process_peaks(cpu_peaks, "cpu");
        return cpu_summary;
    }

    String get_mem_summary(){
        // entry function to get memory summary
        String mem_summary = "memory_summary:";

        // get all the indexes of peaks
        List<Integer> mem_peaks = find_peaks(mem);

        mem_summary += process_peaks(mem_peaks, "mem");
        return mem_summary;
    }

    machine_level_Statics(String key, List<String> date_list, List<Double> cpu_list, List<Double> mem_list)
    {
        this.machine_name = key;
        this.date = date_list;
        this.cpu = toArray(cpu_list);
        this.mem = toArray(mem_list);
        set_idle_threshold();
        set_overload_threshold();
    }
}
