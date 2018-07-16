package it.polimi.mwtech.bigdata.hadoopflights.percdelayhalved;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import it.polimi.mwtech.bigdata.hadoopflights.JobWrapper;

public class HalvedDelayGroupsJobWrapper extends JobWrapper {

	public HalvedDelayGroupsJobWrapper(Configuration conf) throws IOException
	{
		super(conf);
	}
	
	public static class HalvedKeySwapMapper extends Mapper<Text, Text, Text, Text>
	{
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] vals = value.toString().split(",");
			
			Text newKey = new Text(vals[1]);
			Text newValue = new Text(vals[2] + "," + vals[3]);
			
			context.write(newKey, newValue);
		}
	}
	
	public static class HalvedGroupReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int n = 0, d = 0;
			for (Text val : values) {
				String vi = val.toString();
				String[] vis = vi.split(",");
				n += Integer.parseInt(vis[0]);
				d += Integer.parseInt(vis[1]);
			}
			context.write(key, new Text(Integer.toString(n) + "," + Integer.toString(d)));
		}
	}
	
	@Override
	public void setupJob() {
		Job job = getJob();
	    job.setMapperClass(HalvedKeySwapMapper.class);
	    job.setReducerClass(HalvedGroupReducer.class);
	    job.setCombinerClass(HalvedGroupReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	}

}
