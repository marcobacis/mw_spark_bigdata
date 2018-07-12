package it.polimi.mwtech.bigdata.hadoopflights.percweathercanc;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import it.polimi.mwtech.bigdata.hadoopflights.FileParserBase;
import it.polimi.mwtech.bigdata.hadoopflights.JobWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.common.DateUtils;

public class CancelledWeatherJobWrapper extends JobWrapper {
	  
	public CancelledWeatherJobWrapper(Configuration conf) throws IOException
	  {
	    super(conf);
	  }


	  public static class CancelledWeatherMapper extends FileParserBase<Text, Text>
	  {

	    @Override
	    public Text selectKey(Map<FileParserBase.RowKey, String> row)
	    {
	      String y = row.get(FileParserBase.RowKey.YEAR);
	      String m = row.get(FileParserBase.RowKey.MONTH);
	      String d = row.get(FileParserBase.RowKey.DAY_OF_MONTH);
	      return DateUtils.toWeek(y, m, d);
	    }


	    @Override
	    public Text selectValue(Map<FileParserBase.RowKey, String> row)
	    {
	      String canc = row.get(FileParserBase.RowKey.IS_CANCELED);
	      String reason = row.get(FileParserBase.RowKey.CANCELLATION_CODE);
	      
	      if (reason.trim().equalsIgnoreCase("B")) {
	    	  return new Text(canc + ",1");
	      }
	      return new Text("0,1");
	    }

	  }


	  public static class CancelledWeatherReducer extends Reducer<Text, Text, Text, Text>
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
	    job.setMapperClass(CancelledWeatherMapper.class);
	    job.setReducerClass(CancelledWeatherReducer.class);
	    job.setCombinerClass(CancelledWeatherReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	  }
}
