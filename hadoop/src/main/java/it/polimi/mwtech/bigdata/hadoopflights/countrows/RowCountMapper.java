
package it.polimi.mwtech.bigdata.hadoopflights.countrows;

import java.util.*;

import org.apache.hadoop.io.*;

import it.polimi.mwtech.bigdata.hadoopflights.*;


public class RowCountMapper extends FileParserBase<IntWritable, IntWritable>
{

  @Override
  public IntWritable selectKey(Map<FileParserBase.RowKey, String> row)
  {
    return new IntWritable(1);
  }


  @Override
  public IntWritable selectValue(Map<FileParserBase.RowKey, String> row)
  {
    return new IntWritable(1);
  }

}
