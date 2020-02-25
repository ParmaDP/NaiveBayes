package parmanix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceEntrypoint {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		System.out.println(args);
		Configuration conf = new Configuration();

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		conf.set("dataset", args[2]);
		conf.set("untrustedMapperClassName", args[3]);
		conf.set("trustedReducerName", args[4]);
		
		conf.set("epsilon", args[5]);
		conf.set("delta", args[6]);
		conf.set("minRange", args[7]);
		conf.set("maxRange", args[8]);
		
		conf.set("privacyBudget", args[9]);
		conf.set("maxKeyPerGroup", args[10]);
		conf.set("n", args[11]);
		
		Job job = Job.getInstance(conf, "MapReduceEntrypoint"); // write the name of main class here
		job.setJarByClass(MapReduceEntrypoint.class); // write the name of main class here
		job.addFileToClassPath(new Path("/additional_jars/commons-math3-3.6.1.jar"));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);	
		job.setMapperClass(MapperWrapper.class);
		job.setCombinerClass(ParmaCombiner.class);
		job.setReducerClass(ReducerWrapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(2);
//	    job.setInputFormatClass(TextInputFormat.class);              
//	    job.setOutputFormatClass(TextOutputFormat.class);

		// deleting the output path automatically from hdfs so that we don't have to
		// delete it explicitly
		// outputPath.getFileSystem(conf).delete(outputPath);

		// Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
