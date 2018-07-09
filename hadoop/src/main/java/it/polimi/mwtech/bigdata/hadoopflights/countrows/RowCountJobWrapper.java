
package it.polimi.mwtech.bigdata.hadoopflights.countrows;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;

import it.polimi.mwtech.bigdata.hadoopflights.*;


public class RowCountJobWrapper extends JobWrapper
{

  public RowCountJobWrapper(Configuration conf) throws IOException
  {
    super(conf);
  }


  public void setupJob()
  {
    Job job = getJob();
    job.setMapperClass(RowCountMapper.class);
    job.setReducerClass(RowCountReducer.class);
    job.setCombinerClass(RowCountReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
  }

}
