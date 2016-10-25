import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
      //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
      public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
      {
            int sum = 0;
            /*iterates through all the values available with a key and add them together and give the
            final result as the key and sum of its values*/
          while (values.hasNext())
          {
               sum += values.next().get();
          }
          output.collect(key, new IntWritable(sum));
      }
}


/**
Reducer<Text, IntWritable, Text, IntWritable>
The first two refers to data type of Input Key and Value to the reducer and the last two refers to data type of output key and value. 
Our mapper emits output as <apple,1> , <grapes,1> , <apple,1> etc.

This is the input for reducer so here the data types of key and value in java would be String and int, 
the equivalent in Hadoop would be Text and IntWritable. Also we get the output as<word, 
no of occurrences> so the data type of output Key Value would be <Text, IntWritable>

The input to reduce method from the mapper after the sort and shuffle phase would be the key with the list of 
associated values with it. For example here we have multiple values for a single key from our mapper like 
<apple,1> , <apple,1> , <apple,1> , <apple,1> . This key values would be fed into the reducer as < apple, {1,1,1,1} > 


**/
