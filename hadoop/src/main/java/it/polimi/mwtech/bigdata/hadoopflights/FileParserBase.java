
package it.polimi.mwtech.bigdata.hadoopflights;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


abstract public class FileParserBase<KEYOUT, VALUEOUT> extends Mapper<Object, Text, KEYOUT, VALUEOUT>
{
  /* Declaration order must match column order in data files */
  public enum RowKey {
    YEAR,
    MONTH,
    DAY_OF_MONTH,
    DAY_OF_WEEK,
    ACTUAL_LOCAL_DEP_TIME,
    SCHED_LOCAL_DEP_TIME,
    ACTUAL_LOCAL_ARR_TIME,
    SCHED_LOCAL_ARR_TIME,
    CARRIER_ID,
    FLIGTH_NUMBER,
    PLANE_TAIL_NUMBER,
    ACTUAL_ELAPSED_MINS,
    SCHED_ELAPSED_MINS,
    AIR_MINS,
    ARR_DELAY_MINS,
    DEP_DELAY_MINS,
    ORIGIN_IATA_ID,
    DEST_IATA_ID,
    DISTANCE_MILES,
    TAXI_MINS_IN,
    TAXI_MINS_OUT,
    IS_CANCELED,
    CANCELLATION_CODE,
    IS_DIVERTED,
    CARRIER_DELAY_MINS,
    WEATHER_DELAY_MINS,
    NAS_DELAY_MINS,
    SEC_DELAY_MINS,
    LATE_AIRCRAFT_DELAY_MINS
  };
  
  
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException
  {
    String sval = value.toString();
    String rawvals[] = sval.split(",");
    RowKey keys[] = RowKey.values();
    
    HashMap<RowKey, String> row = new HashMap<RowKey, String>();
    int count = Integer.min(rawvals.length, keys.length);
    
    for (int i=0; i<count; i++) {
      row.put(keys[i], rawvals[i].trim());
    }
    
    KEYOUT keyout = selectKey(row);
    VALUEOUT valout = selectValue(row);
    context.write(keyout, valout);
  }


  abstract public KEYOUT selectKey(Map<RowKey, String> row);
  
  
  abstract public VALUEOUT selectValue(Map<RowKey, String> row);

}
