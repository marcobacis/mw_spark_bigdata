package it.polimi.mwtech.bigdata.hadoopflights.weeklypenalty;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import it.polimi.mwtech.bigdata.hadoopflights.FileParserBase;
import it.polimi.mwtech.bigdata.hadoopflights.JobWrapper;
import it.polimi.mwtech.bigdata.hadoopflights.FileParserBase.RowKey;
import it.polimi.mwtech.bigdata.hadoopflights.common.Utils;

public class WeeklyPenaltyJobWrapper extends JobWrapper {

	public WeeklyPenaltyJobWrapper(Configuration conf) throws IOException  
	{
		super(conf);
	}

	/* Copied from FileParserBase to allow to write more than one record per row (0 to 2) */
	public static class WeeklyPenaltyMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		private HashMap<RowKey, String> parseRow(String rowstring)
		{
			String rawvals[] = rowstring.split(",");
			RowKey keys[] = RowKey.values();
			
			HashMap<RowKey, String> row = new HashMap<RowKey, String>();
			int count = Integer.min(rawvals.length, keys.length);

			for (int i = 0; i < count; i++) {
				row.put(keys[i], rawvals[i].trim());
			}
			
			return row;
		}
		
		public void map(LongWritable linenum, Text value, Context context) throws IOException, InterruptedException
		{
			if (linenum.get() == 0)
				return;

			HashMap<RowKey, String> row = parseRow(value.toString());
			
			String origin = row.get(RowKey.ORIGIN_IATA_ID);
			String dest = row.get(RowKey.DEST_IATA_ID);
			
			String arrdelay = row.get(RowKey.ARR_DELAY_MINS).trim();
			String depdelay = row.get(RowKey.DEP_DELAY_MINS).trim();
			
			String y = row.get(FileParserBase.RowKey.YEAR);
			String m = row.get(FileParserBase.RowKey.MONTH);
			String d = row.get(FileParserBase.RowKey.DAY_OF_MONTH);
			String week = Utils.toWeek(y, m, d).toString();
			
			if(!arrdelay.equals("NA")) {
				Text newKey = new Text(week + "," + dest);
				Text penalty = new Text(Integer.parseInt(arrdelay) > 15 ? "0.5" : "0.0");
				context.write(newKey, penalty);
			}
			
			if(!depdelay.equals("NA")) {
				Text newKey = new Text(week + "," + origin);
				Text penalty = new Text(Integer.parseInt(depdelay) > 15 ? "1.0" : "0.0");
				context.write(newKey, penalty);
			}
			
		}

	}


	public static class WeeklyPenaltyReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text k, Iterable<Text> v, Context context) throws IOException, InterruptedException
		{
			float tot = 0;
			for (Text i : v) {
				float pen = Float.parseFloat(i.toString());
				tot += pen;
				
			}
			context.write(k, new Text(Float.toString(tot)));
		}
	}

	@Override
	public void setupJob()
	{	
		Job job = getJob();
		job.setMapperClass(WeeklyPenaltyMapper.class);
		job.setReducerClass(WeeklyPenaltyReducer.class);
		job.setCombinerClass(WeeklyPenaltyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

	}

}