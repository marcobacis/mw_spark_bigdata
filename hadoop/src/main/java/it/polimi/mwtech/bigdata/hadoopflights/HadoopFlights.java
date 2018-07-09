
package it.polimi.mwtech.bigdata.hadoopflights;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import it.polimi.mwtech.bigdata.hadoopflights.common.FracToPercentJobWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.countrows.*;
import it.polimi.mwtech.bigdata.hadoopflights.perccancperday.*;


public class HadoopFlights
{

  static void computeRowCount(Configuration conf, String in, String out) throws Exception
  {
    RowCountJobWrapper rcjw = new RowCountJobWrapper(conf);
    rcjw.setOutputPath(new Path(out));
    rcjw.setInputPaths(new Path(in));
    rcjw.submitJobAndChain();
    rcjw.waitForCompletion();
  }


  static void computePercCancelledFlightsPerDay(Configuration conf, String in, String out) throws Exception
  {
    CancelledFlightsFractionJobWrapper cffjw = new CancelledFlightsFractionJobWrapper(conf);
    FracToPercentJobWrapper ftpjw = new FracToPercentJobWrapper(conf);
    cffjw.feedOutputToJob(ftpjw);
    ftpjw.setOutputPath(new Path(out));
    cffjw.setInputPaths(new Path(in));
    ftpjw.submitJobAndChain();
    ftpjw.waitForCompletion();
  }


  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    // computeRowCount(conf, args[0], args[1] + "_rc");
    computePercCancelledFlightsPerDay(conf, args[0], args[1] + "_pcfpd");
  }

}
