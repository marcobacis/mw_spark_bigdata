package it.polimi.mwtech.bigdata.hadoopflights.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.hadoop.io.Text;

public class Utils {

	static SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * From given date to monday of the same week
	 * @param year
	 * @param month
	 * @param day
	 * @return the date string of the first day of the week for the given date
	 */
	public static Text toWeek(String year, String month, String day) {
			        
	    Calendar cal = Calendar.getInstance();
	    
	    cal.setFirstDayOfWeek(Calendar.MONDAY);
	    
	    cal.set(Integer.parseInt(year), Integer.parseInt(month)-1, Integer.parseInt(day));
	    
	    int distance = cal.get(Calendar.DAY_OF_WEEK) - cal.getFirstDayOfWeek();
	    if(distance < 0)
	    	distance = distance + 7;
	    
	    cal.add(Calendar.DATE, -distance);
	    
	    return new Text(dateformat.format(cal.getTime()));
	}
}
