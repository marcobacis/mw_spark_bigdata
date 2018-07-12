package it.polimi.mwtech.bigdata.hadoopflights.percdelayhalved;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import it.polimi.mwtech.bigdata.hadoopflights.FileParserBase;
import it.polimi.mwtech.bigdata.hadoopflights.JobWrapper;

public class FracDistanceGroupHalvedWrapper extends JobWrapper{

	public FracDistanceGroupHalvedWrapper(Configuration conf) throws IOException
	  {
	    super(conf);
	  }


	public static class HalvedDelayMapper extends FileParserBase<Text, Text>
	{

	    @Override
	    public Text selectKey(Map<FileParserBase.RowKey, String> row)
	    {
	      
	    	int distance;
	    	
	    	try {
	    		distance = Integer.parseInt(row.get(FileParserBase.RowKey.DISTANCE_MILES));
	    		
	    		Integer group = distance / 200;
		    	
		    	return new Text(group.toString());
		    	
	    	} catch (Exception e) {
	    		return null;
	    	}
	    	
	    }


	    @Override
	    public Text selectValue(Map<FileParserBase.RowKey, String> row)
	    {
	    	Integer valid = 1;
	    	
	    	int depdelay = 0;
	    	int arrdelay = 0;
	    	
	    	try {
		    	depdelay = Integer.parseInt(row.get(FileParserBase.RowKey.DEP_DELAY_MINS));
		    	arrdelay = Integer.parseInt(row.get(FileParserBase.RowKey.ARR_DELAY_MINS));
	    	} catch (Exception e) {
	    		valid = 0;
	    	}
	    	
	    	Integer halved = 0;
	    	
	    	if(arrdelay <= depdelay * 0.5)
	    		halved = 1;
	    	
	    	return new Text(new String(halved + "," + valid));
	    }

	}


	  public static class HalvedDelayReducer extends Reducer<Text, Text, Text, Text>
	  {
	    @Override
	    protected void reduce(Text k, Iterable<Text> v, Context ctxt) throws IOException, InterruptedException
	    {
	      int n = 0, d = 0;
	      for (Text i : v) {
	        String vi = i.toString();
	        String[] vis = vi.split(",");
	        n += Integer.parseInt(vis[0]);
	        d += Integer.parseInt(vis[1]);
	      }
	      ctxt.write(k, new Text(Integer.toString(n) + "," + Integer.toString(d)));
	    }
	  }


	  @Override
	  public void setupJob()
	  {
	    Job job = getJob();
	    job.setMapperClass(HalvedDelayMapper.class);
	    job.setReducerClass(HalvedDelayReducer.class);
	    job.setCombinerClass(HalvedDelayReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	  }
}
