package it.polimi.mwtech.bigdata.hadoopflights.perccancperday;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import it.polimi.mwtech.bigdata.hadoopflights.*;


public class CancelledFlightsFractionJobWrapper extends JobWrapper
{

  public CancelledFlightsFractionJobWrapper(Configuration conf) throws IOException
  {
    super(conf);
  }


  public static class CancelledInfoMapper extends FileParserBase<Text, Text>
  {

    @Override
    public Text selectKey(Map<FileParserBase.RowKey, String> row)
    {
      String y = row.get(FileParserBase.RowKey.YEAR);
      String m = row.get(FileParserBase.RowKey.MONTH);
      String d = row.get(FileParserBase.RowKey.DAY_OF_MONTH);
      if (y == null || m == null || d == null)
        return null;
      return new Text(y + "-" + m + "-" + d);
    }


    @Override
    public Text selectValue(Map<FileParserBase.RowKey, String> row)
    {
      String canc = row.get(FileParserBase.RowKey.IS_CANCELED);
      return new Text(canc + ",1");
    }

  }


  public static class CancelledInfoReducer extends Reducer<Text, Text, Text, Text>
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
    job.setMapperClass(CancelledInfoMapper.class);
    job.setReducerClass(CancelledInfoReducer.class);
    job.setCombinerClass(CancelledInfoReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
  }

}
