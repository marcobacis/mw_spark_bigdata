package it.polimi.mwtech.bigdata.hadoopflights.percdelayhalved;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.*;

import it.polimi.mwtech.bigdata.hadoopflights.JobWrapper;

public class DistFlightsJoinWrapper extends JobWrapper {

	public DistFlightsJoinWrapper(Configuration conf)  throws IOException {
		super(conf);
	}
	
	
	public static class DistFlightReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			int tot = 0;
			int halved = 0;
			
			String group = "";
			
			for(Text val : values) {
				String[] parts = val.toString().split(",");
				
				if(parts[0].equals("distance")) { //from unique path-group job
					group = parts[1];
				} else if(parts[0].equals("delay")) { //from delay halved job
					halved += Integer.parseInt(parts[1]);
					tot += Integer.parseInt(parts[2]);
				} else if(parts[0].equals("reduced")) { //from past reduce
					if(!parts[1].isEmpty()) group = parts[1];
					halved += Integer.parseInt(parts[2]);
					tot += Integer.parseInt(parts[3]);
				}
			}
			
			context.write(key, new Text("reduced," + group + "," + halved + "," + tot));
		}
	}
	
	
	@Override
	public void setupJob() {
		Job job = getJob();
	    job.setReducerClass(DistFlightReducer.class);
	    job.setCombinerClass(DistFlightReducer.class);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	}

}
