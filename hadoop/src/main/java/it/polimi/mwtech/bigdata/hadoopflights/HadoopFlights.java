
package it.polimi.mwtech.bigdata.hadoopflights;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import it.polimi.mwtech.bigdata.hadoopflights.countrows.*;


public class HadoopFlights
{

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    RowCountJobWrapper rcjw = new RowCountJobWrapper(conf);
    rcjw.setOutputPath(new Path(args[1]));
    rcjw.setInputPaths(new Path(args[0]));
    rcjw.submitJobAndChain();
    rcjw.waitForCompletion();
  }

}
