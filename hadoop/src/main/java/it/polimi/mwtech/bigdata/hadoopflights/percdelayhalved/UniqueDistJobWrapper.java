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

/**
 * MapReduce job used to select all the paths present in the dataset,
 * along with the distance between the airports.
 */
public class UniqueDistJobWrapper extends JobWrapper {

	public UniqueDistJobWrapper(Configuration conf) throws IOException
	  {
	    super(conf);
	  }


	public static class UniqueDistMapper extends FileParserBase<Text, Text>
	{

	    @Override
	    public Text selectKey(Map<FileParserBase.RowKey, String> row)
	    {
	      	    	
	    	try {
	    		int distance = Integer.parseInt(row.get(FileParserBase.RowKey.DISTANCE_MILES));
	    		
	    		String dep = row.get(FileParserBase.RowKey.ORIGIN_IATA_ID);
	    		String dest = row.get(FileParserBase.RowKey.DEST_IATA_ID);
	    		
		    	return new Text(Utils.pathKey(dep, dest));
		    	
	    	} catch (Exception e) {
	    		return null;
	    	}
	    	
	    }


	    @Override
	    public Text selectValue(Map<FileParserBase.RowKey, String> row)
	    {
	    	String val;
	    	
	    	try{
	    		Integer distance = Integer.parseInt(row.get(FileParserBase.RowKey.DISTANCE_MILES));
	    		Integer group = distance / 200;
	    		
	    		val = group.toString();
	    	}catch(Exception e) {
	    		val = "NA";
	    	}
	    	
	    	return new Text("distance," + val);
	    }

	}


	public static class UniqueDistReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text k, Iterable<Text> v, Context ctxt) throws IOException, InterruptedException
		{	      
			for (Text i : v) {
				String[] vis = i.toString().split(",");
				if(vis[1] != "NA") {
					ctxt.write(k, new Text("distance," + vis[1]));
					break;
				}
			}
		}
	}


	  @Override
	  public void setupJob()
	  {
	    Job job = getJob();
	    job.setMapperClass(UniqueDistMapper.class);
	    job.setReducerClass(UniqueDistReducer.class);
	    job.setCombinerClass(UniqueDistReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	  }
}
