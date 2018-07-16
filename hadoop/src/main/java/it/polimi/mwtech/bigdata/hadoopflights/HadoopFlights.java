
package it.polimi.mwtech.bigdata.hadoopflights;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import it.polimi.mwtech.bigdata.hadoopflights.common.FracToPercentJobWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.countrows.*;
import it.polimi.mwtech.bigdata.hadoopflights.perccancperday.*;
import it.polimi.mwtech.bigdata.hadoopflights.percdelayhalved.DistFlightsJoinWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.percdelayhalved.HalvedDelayGroupsJobWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.percdelayhalved.HalvedDelayJobWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.percdelayhalved.UniqueDistJobWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.percweathercanc.*;
import it.polimi.mwtech.bigdata.hadoopflights.weeklypenalty.WeeklyPenaltyJobWrapper;


public class HadoopFlights
{

	static void computeRowCount(Configuration conf, String in, String out) throws Exception
	{
		RowCountJobWrapper rcjw = new RowCountJobWrapper(conf);
		rcjw.setOutputPath(new Path(out));
		rcjw.addInputPaths(new Path(in));
		rcjw.submitJobAndChain();
		rcjw.waitForCompletion();
	}


	static void computePercCancelledFlightsPerDay(Configuration conf, String in, String out) throws Exception
	{
		CancelledFlightsFractionJobWrapper cffjw = new CancelledFlightsFractionJobWrapper(conf);
		FracToPercentJobWrapper ftpjw = new FracToPercentJobWrapper(conf);
		cffjw.feedOutputToJob(ftpjw);
		ftpjw.setOutputPath(new Path(out));
		cffjw.addInputPaths(new Path(in));
		ftpjw.submitJobAndChain();
		ftpjw.waitForCompletion();
	}

	static void computePercCancelledPerWeekDueToWeather(Configuration conf, String in, String out) throws Exception
	{
		CancelledWeatherJobWrapper pwcpw = new CancelledWeatherJobWrapper(conf);
		FracToPercentJobWrapper ftpjw = new FracToPercentJobWrapper(conf);
		pwcpw.feedOutputToJob(ftpjw);
		ftpjw.setOutputPath(new Path(out));
		pwcpw.addInputPaths(new Path(in));
		ftpjw.submitJobAndChain();
		ftpjw.waitForCompletion();
	}

	/**
	 * Third query
	 * 
	 *  UniqueDist     HalvedDelay
	 *        \          /
	 *       DistFlightJoin
	 *             |
	 *     HalvedDelayGroups
	 *             |
	 *       FracToPercent
	 */
	static void computeUniqueDistances(Configuration conf, String in, String out) throws Exception
	{
		UniqueDistJobWrapper unique = new UniqueDistJobWrapper(conf);
		HalvedDelayJobWrapper halved = new HalvedDelayJobWrapper(conf);
		DistFlightsJoinWrapper join = new DistFlightsJoinWrapper(conf);
		HalvedDelayGroupsJobWrapper groups = new HalvedDelayGroupsJobWrapper(conf);
		FracToPercentJobWrapper ftpjw = new FracToPercentJobWrapper(conf);

		halved.feedOutputToJob(join);
		unique.feedOutputToJob(join);
		join.feedOutputToJob(groups);
		groups.feedOutputToJob(ftpjw);

		halved.addInputPaths(new Path(in));
		unique.addInputPaths(new Path(in));

		ftpjw.setOutputPath(new Path(out));

		ftpjw.submitJobAndChain();
		ftpjw.waitForCompletion();
	}

	/**
	 * Fourth query. Weekly penalty score for each airport
	 *  a weekly "penalty" score for each airport that depends on both the
	 *  its incoming and outgoing flights.
	 *  The score adds 0.5 for each incoming flight that is more than 15 minutes late,
	 *  and 1 for each outgoing flight that is more than 15 minutes late.
	 *  
	 *  There is only one job
	 *  
	 */
	static void computeWeeklyPenalty(Configuration conf, String in, String out) throws Exception
	{
		WeeklyPenaltyJobWrapper penalty = new WeeklyPenaltyJobWrapper(conf);
		
		penalty.addInputPaths(new Path(in));
		penalty.setOutputPath(new Path(out));
		
		penalty.submitJobAndChain();
		penalty.waitForCompletion();
	}


	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		// computeRowCount(conf, args[0], args[1] + "_rc");
		computePercCancelledFlightsPerDay(conf, args[0], args[1] + "pcfpd.csv");
		computePercCancelledPerWeekDueToWeather(conf, args[0], args[1] + "pwcpw.csv");
		computeUniqueDistances(conf, args[0], args[1] + "joined.csv");
		computeWeeklyPenalty(conf, args[0], args[1] + "ppa.csv");
	}

}
