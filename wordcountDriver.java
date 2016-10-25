import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
By implementing the Tool interface and extending Configured class, 
you can easily set your hadoop Configuration object via the GenericOptionsParser, 
thus through the command line interface. 
This makes your code definitely more portable (and additionally slightly cleaner) 
as you do not need to hardcode any specific configuration anymore.
**/

public class WordCount extends Configured implements Tool{
      public int run(String[] args) throws Exception
      {
            // Create configuration
			Configuration conf = new Configuration();
 
			// Create job
			Job job = new Job(conf, "Tool Job");
			job.setJarByClass(ToolMapReduce.class);

            //Setting configuration object with the Data Type of output Key and Value
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);

            //Providing the mapper and reducer class names
            conf.setMapperClass(WordCountMapper.class);
            conf.setReducerClass(WordCountReducer.class);

            //the hdfs input and output directory to be fetched from the command line
            FileInputFormat.addInputPath(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

            // Execute job
			int code = job.waitForCompletion(true) ? 0 : 1;
			System.exit(code);
      }
     
      public static void main(String[] args) throws Exception
      {
            int res = ToolRunner.run(new Configuration(), new WordCount(),args);
            System.exit(res);
      }
}
