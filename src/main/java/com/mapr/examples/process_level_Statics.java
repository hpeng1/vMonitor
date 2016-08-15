package com.mapr.examples;  // for local IDE
//package com.test.wordcount;   // for hadoop

import java.util.*;

// this class helps calculate statistics of mapreduce results

public class process_level_Statics
{
    double[] data;
    public int size;

    public process_level_Statics(double[] data)
    {
        this.data = data;
        size = data.length;
    }

    double getMean()
    {
        double sum = 0.0;
        for(double a : data)
            sum += a;
        return sum/size;
    }

    double getMax(){
        double max = 0.0;
        for(double a : data)
        {
            if(a > max)
                max = a;
        }
        return max;
    }

    double getVariance()
    {
        double mean = getMean();
        double temp = 0;
        for(double a :data)
            temp += (a-mean)*(a-mean);
        return temp/size;
    }

    double getStdDev()
    {
        return Math.sqrt(getVariance());
    }

    public double getMedian()
    {
        Arrays.sort(data);

        if (data.length % 2 == 0)
        {
            return (data[(data.length / 2) - 1] + data[data.length / 2]) / 2.0;
        }
        else
        {
            return data[data.length / 2];
        }
    }
}