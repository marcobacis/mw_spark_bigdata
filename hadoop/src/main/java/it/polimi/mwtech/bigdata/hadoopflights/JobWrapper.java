
package it.polimi.mwtech.bigdata.hadoopflights;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public abstract class JobWrapper
{
  Job myJob;
  ArrayList<JobWrapper> prevJobs;
  Thread execThread;
  private Path[] inputPaths;
  private Path outputPath;


  public JobWrapper(Configuration conf) throws IOException
  {
    prevJobs = new ArrayList<JobWrapper>();
    myJob = Job.getInstance(conf, this.getClass().getName());
    myJob.setJarByClass(JobWrapper.class);
    setupJob();
  }


  public Job getJob()
  {
    return myJob;
  }


  public void setInputPaths(Path... p) throws IOException
  {
    FileInputFormat.setInputPaths(myJob, p);
    inputPaths = p;
  }


  public Path[] getInputPath()
  {
    return inputPaths;
  }


  public void setOutputPath(Path p)
  {
    FileOutputFormat.setOutputPath(myJob, p);
    outputPath = p;
  }


  public Path getOutputPath()
  {
    return outputPath;
  }


  abstract public void setupJob();


  public void feedOutputToJob(JobWrapper destJob) throws IOException
  {
    myJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    String id = this.getClass().getName() + "_" + UUID.randomUUID().toString() + ".tmp";
    setOutputPath(new Path("tmp/" + id));
    destJob.takeInputFromJob(this);
  }


  void takeInputFromJob(JobWrapper srcJob) throws IOException
  {
    prevJobs.add(srcJob);
    myJob.setInputFormatClass(SequenceFileInputFormat.class);
    setInputPaths(srcJob.getOutputPath());
  }


  void executeSubmitJobAndChain()
  {
    try {
      for (JobWrapper prev : prevJobs) {
        prev.submitJobAndChain();
      }
      for (JobWrapper prev : prevJobs) {
        prev.waitForCompletion();
      }

      myJob.submit();
      myJob.waitForCompletion(true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public void submitJobAndChain() throws Exception
  {
    if (execThread != null)
      throw new Exception("job already submitted");
    execThread = new Thread(() -> this.executeSubmitJobAndChain());
    execThread.start();
  }


  public void waitForCompletion() throws Exception
  {
    if (execThread == null)
      throw new Exception("job not submitted");
    execThread.join();
  }
}
