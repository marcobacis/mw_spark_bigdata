package it.polimi.mwtech.bigdata.hadoopflights.flightperpath;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import it.polimi.mwtech.bigdata.hadoopflights.FileParserBase;
import it.polimi.mwtech.bigdata.hadoopflights.JobWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.common.Utils;

public class FlightsPerPathMonthly extends JobWrapper{

	public FlightsPerPathMonthly(Configuration conf) throws IOException
	  {
	    super(conf);
	  }

	public static class PathsMapper extends FileParserBase<Text, IntWritable>
	{

		private final static IntWritable one = new IntWritable(1);
		
	    @Override
	    public Text selectKey(Map<FileParserBase.RowKey, String> row)
	    {
	    	return new Text(Utils.pathKey(row,",")
	    			        + "," + row.get(RowKey.YEAR)
	    			        + "," + row.get(RowKey.MONTH));
	    }


	    @Override
	    public IntWritable selectValue(Map<FileParserBase.RowKey, String> row)
	    {	
	    	return one;
	    }

	}


	  public static class PathsReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	  {
	    @Override
	    protected void reduce(Text k, Iterable<IntWritable> v, Context ctxt) throws IOException, InterruptedException
	    {
	      int sum = 0;
	      for (IntWritable i : v) {
	    	  sum += i.get();
	      }
	      ctxt.write(k, new IntWritable(sum));
	    }
	  }


	  @Override
	  public void setupJob()
	  {
	    Job job = getJob();
	    job.setMapperClass(PathsMapper.class);
	    job.setReducerClass(PathsReducer.class);
	    job.setCombinerClass(PathsReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	  }
}
