
package it.polimi.mwtech.bigdata.hadoopflights.countrows;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class RowCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{
  
  @Override
  protected void reduce(IntWritable k, Iterable<IntWritable> v, Context ctxt) throws IOException, InterruptedException
  {
    int res = 0;
    for (IntWritable n : v) {
      res += n.get();
    }
    ctxt.write(new IntWritable(1), new IntWritable(res));
  }
  
}
