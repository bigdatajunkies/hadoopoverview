import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
      //hadoop supported data types
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
     
      //map method that performs the tokenizer job and framing the initial key value pairs
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
      {
            //taking one line at a time and tokenizing the same
            String line = value.toString();
          StringTokenizer tokenizer = new StringTokenizer(line);
         
          //iterating through all the words available in that line and forming the key value pair
            while (tokenizer.hasMoreTokens())
            {
               word.set(tokenizer.nextToken());
               //sending to output collector which inturn passes the same to reducer
                 output.collect(word, one);
            }
       }
}


/**
 Mapper<LongWritable, Text, Text, IntWritable> , 
 it refers to the data type of input and output key value pairs specific to the mapper or rateher the map method,
 ie Mapper<Input Key Type, Input Value Type, Output Key Type, Output Value Type>
 
 the input to a mapper is a single line, so this Text (one input line) forms the input value. 
 The input key would a long value assigned in default based on the position of Text in input file. 
 Our output from the mapper is of the format “Word, 1“ hence the data type of our 
 output key value pair is <Text(String),  IntWritable(int)>
**/
