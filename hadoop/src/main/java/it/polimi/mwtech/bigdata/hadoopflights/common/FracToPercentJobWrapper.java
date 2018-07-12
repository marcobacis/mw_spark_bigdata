
package it.polimi.mwtech.bigdata.hadoopflights.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import it.polimi.mwtech.bigdata.hadoopflights.JobWrapper;


public class FracToPercentJobWrapper extends JobWrapper
{

  public FracToPercentJobWrapper(Configuration conf) throws IOException
  {
    super(conf);
  }


  public static class FracToPercentMapper extends Mapper<Text, Text, Text, FloatWritable>
  {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] nd = value.toString().split(",");
      float n = Integer.parseInt(nd[0]);
      float d = Integer.parseInt(nd[1]);
      context.write(key, new FloatWritable((n / d) * 100));
    }
  }


  @Override
  public void setupJob()
  {
    Job job = getJob();
    job.setMapperClass(FracToPercentMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
  }

}
