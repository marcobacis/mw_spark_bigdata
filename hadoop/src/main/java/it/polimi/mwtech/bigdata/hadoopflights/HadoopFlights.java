
package it.polimi.mwtech.bigdata.hadoopflights;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import it.polimi.mwtech.bigdata.hadoopflights.common.FracToPercentJobWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.countrows.*;
import it.polimi.mwtech.bigdata.hadoopflights.flightperpath.FlightsPerPathMonthly;
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

	/**
	 * First query
	 * "The percentage of canceled flights per day,
	 * throughout the entire data set."
	 * 
	 */
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

	
	/**
	 * Second query.
	 * "Weekly percentages of delays that are due to weather,
	 * throughout the entire data set."
	 * 
	 */
	static void computePercWeatherCancellationPerWeek(Configuration conf, String in, String out) throws Exception
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
	 * "The percentage of flights belonging to a given 'distance group'
	 * that were able to halve their departure delays by the time they
	 * arrived at their destinations. Distance groups assort flights
	 * by their total distance in miles (at 200 miles steps)."
	 * 
	 * The UniqueDist job results are joined with the other job in order
	 * to fix some missing distances (actually, we directly deal with the
	 * distance group instead of the distance in miles).
	 * 
	 *  UniqueDist     HalvedDelay
	 *        \          /
	 *       DistFlightJoin
	 *             |
	 *     HalvedDelayGroups
	 *             |
	 *       FracToPercent
	 */
	static void computePercDepDelayHalvedPerGroup(Configuration conf, String in, String out) throws Exception
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
	 * Fourth query.
	 *  "A weekly 'penalty' score for each airport that depends on both the
	 *  its incoming and outgoing flights.
	 *  The score adds 0.5 for each incoming flight that is more than 15 minutes late,
	 *  and 1 for each outgoing flight that is more than 15 minutes late."
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
	
	/**
	 * Additional query. Counts the number of flights on every
	 * pair of airports with at least one flight, for each month.
	 */
	static void computeFlightsPerPathMonthly(Configuration conf, String in, String out) throws Exception
	{
		FlightsPerPathMonthly fppm = new FlightsPerPathMonthly(conf);
		
		fppm.addInputPaths(new Path(in));
		fppm.setOutputPath(new Path(out));
		
		fppm.submitJobAndChain();
		fppm.waitForCompletion();
	}


	/**
	 * args[1] => Input file path
	 * args[2] => Output files directory path
	 */
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		//computeRowCount(conf, args[0], args[1] + "/rowcount.csv");
		computePercCancelledFlightsPerDay(conf, args[0], args[1] + "/pcfpd.csv");
		computePercWeatherCancellationPerWeek(conf, args[0], args[1] + "/pwcpw.csv");
		computePercDepDelayHalvedPerGroup(conf, args[0], args[1] + "/pddhpg.csv");
		computeWeeklyPenalty(conf, args[0], args[1] + "/ppa.csv");
		computeFlightsPerPathMonthly(conf, args[0], args[1] + "/fppm.csv");
	}

}
