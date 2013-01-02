import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class NgramsDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(NgramsMapper.class);
		job.setCombinerClass(NgramsReducer.class);
		job.setReducerClass(NgramsReducer.class);
		
		job.setOutputKeyClass(TextTriple.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(10);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new NgramsDriver(), args);
		System.exit(exitCode);
	}
}
