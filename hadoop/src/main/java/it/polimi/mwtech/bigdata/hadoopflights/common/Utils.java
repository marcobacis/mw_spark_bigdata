package it.polimi.mwtech.bigdata.hadoopflights.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.hadoop.io.Text;

import it.polimi.mwtech.bigdata.hadoopflights.FileParserBase;
import it.polimi.mwtech.bigdata.hadoopflights.FileParserBase.RowKey;

public class Utils {

	static SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * From given date to monday of the same week
	 * @param year
	 * @param month
	 * @param day
	 * @param weekDay
	 * @return the date string of the first day of the week for the given date
	 */
	public static Text toWeek(String year, String month, String day, String weekDay) {
			        
	    Calendar cal = Calendar.getInstance();
	    
	    cal.set(Integer.parseInt(year), Integer.parseInt(month)-1, Integer.parseInt(day));
	    
	    int distance = Integer.parseInt(weekDay) - 1;
	    
	    cal.add(Calendar.DATE, -distance);
	    
	    return new Text(dateformat.format(cal.getTime()));
	}
	
	public static Text toWeek(Map<RowKey, String> row) {
        
		String y = row.get(FileParserBase.RowKey.YEAR);
		String m = row.get(FileParserBase.RowKey.MONTH);
		String d = row.get(FileParserBase.RowKey.DAY_OF_MONTH);
		String wd = row.get(FileParserBase.RowKey.DAY_OF_WEEK);
		
	    return Utils.toWeek(y, m, d, wd);
	}
	
	
	public static String pathKey(Map<RowKey, String> row) {
		return pathKey(row,"-");
	}
	
	public static String pathKey(Map<RowKey, String> row, String separator) {
		String dep = row.get(FileParserBase.RowKey.ORIGIN_IATA_ID).trim();
		String dest = row.get(FileParserBase.RowKey.DEST_IATA_ID).trim();
		
		if(dep.compareTo(dest) > 0) {
			return dest + separator + dep;
		}
		
		return dep + separator + dest;
	}
}
