
package it.polimi.mwtech.bigdata.hadoopflights;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;

import it.polimi.mwtech.bigdata.hadoopflights.countrows.*;


public class HadoopFlights
{

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(HadoopFlights.class);
    job.setMapperClass(RowCountMapper.class);
    job.setReducerClass(RowCountReducer.class);
    job.setCombinerClass(RowCountReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    job.submit();
    job.waitForCompletion(true);
  }

}
