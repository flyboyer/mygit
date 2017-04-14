package mr1;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Average {
	
	public static class ScoreMap 
			extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			System.out.println(key);
			System.out.println(line);
			StringTokenizer tokenizer = new StringTokenizer(line,"\n");
			while( tokenizer.hasMoreTokens() )
			{
				StringTokenizer  tokenizerLine = new StringTokenizer(tokenizer.nextToken());
				String strName = tokenizerLine.nextToken();
				String strScore = tokenizerLine.nextToken();
				
				Text name = new Text(strName);
				IntWritable score =new IntWritable(Integer.parseInt(strScore));
				
				context.write(name, score);
			}
			
		}
	}
	
	public static class ScoreReduce
			extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key , Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			int count = 0;
			Iterator<IntWritable> iterator =  values.iterator();
			while(iterator.hasNext())
			{
				sum += iterator.next().get();
				count++;
			}
			int average = (int)sum/count;
			context.write(key,new IntWritable(average));
		}
	}

	
	public static void main(String[] args) throws Exception {
		//System.setProperty("hadoop.home.dir", "F:\\hadoop-2.7.0");
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: wordcount <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Path path = new Path(otherArgs[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
	    FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
	    if (fileSystem.exists(path)) {  
	        fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
	    }  
	    Job job = Job.getInstance(conf, "averageScore");
	    job.setJarByClass(Average.class);
	    job.setMapperClass(ScoreMap.class);
	    job.setCombinerClass(ScoreReduce.class);
	    job.setReducerClass(ScoreReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
	      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	    }
	    FileOutputFormat.setOutputPath(job,
	      new Path(otherArgs[otherArgs.length - 1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}
