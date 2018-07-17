package it.polimi.mwtech.bigdata.hadoopflights.percdelayhalved;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import it.polimi.mwtech.bigdata.hadoopflights.FileParserBase;
import it.polimi.mwtech.bigdata.hadoopflights.JobWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.common.Utils;

public class HalvedDelayJobWrapper extends JobWrapper{

	public HalvedDelayJobWrapper(Configuration conf) throws IOException
	  {
	    super(conf);
	  }

	public static class HalvedDelayMapper extends FileParserBase<Text, Text>
	{

	    @Override
	    public Text selectKey(Map<FileParserBase.RowKey, String> row)
	    {
	    	return new Text(Utils.pathKey(row));
	    }


	    @Override
	    public Text selectValue(Map<FileParserBase.RowKey, String> row)
	    {	
	    	try {
		    	int depdelay = Integer.parseInt(row.get(FileParserBase.RowKey.DEP_DELAY_MINS));
		    	int arrdelay = Integer.parseInt(row.get(FileParserBase.RowKey.ARR_DELAY_MINS));
		    	
		    	String halved = arrdelay <= depdelay /2 ? "1" : "0";
		    	
		    	return new Text("delay," + halved + ",1");
	    	} catch (Exception e) {
	    		return new Text("delay,0,0");
	    	}
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
	        n += Integer.parseInt(vis[1]);
	        d += Integer.parseInt(vis[2]);
	      }
	      ctxt.write(k, new Text("delay," + Integer.toString(n) + "," + Integer.toString(d)));
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
