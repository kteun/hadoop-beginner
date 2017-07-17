package wikibooks.hadoop.chapter05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DelayCountWithCounter extends Configured implements Tool{
	public static void main(String args[])throws Exception
	{
		int res = ToolRunner.run(new Configuration(),new DelayCountWithCounter(),args);
	}
	public int run(String[]args)throws Exception{
	String[]otherArgs=new GenericOptionsParser(getConf(),args).getRemainingArgs();
	if(otherArgs.length!=2) {
		System.err.println("Usage: DelayCountWithCounter<in><out>");
		System.exit(2);
	}
	Job job=new Job(getConf(),"DelayCountWithCounter");
	
	FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
	
	job.setJarByClass(DelayCountWithCounter.class);
	job.setMapperClass(DelayCountMapperWithCounter.class);
	job.setReducerClass(DelayCountReducer.class);
	
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	job.waitForCompletion(true);
	return 0;
	}
}
