package parmanix;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SampleReducer extends Reducer<String, DoubleWritable, String, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

	public void reduce(String key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (DoubleWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
