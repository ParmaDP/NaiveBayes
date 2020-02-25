package parmanix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParmaCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(Text groupedKey, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("Begin combine");
		final Configuration configuration = context.getConfiguration();
		final Double minRange = Double.parseDouble(configuration.get("minRange"));
		final Double maxRange = Double.parseDouble(configuration.get("maxRange"));
		final Double average = (maxRange - minRange ) / 2.0D;
		Integer maxOutputPerGroup = Integer.parseInt(context.getConfiguration().get("n"));
		int i = 0;
		for (DoubleWritable d: values) {
			// At most, expose n values of this mapper's result
			if (i == maxOutputPerGroup)
				break;
			// Range enforcement on each output
			if ( d.get() < minRange || d.get() > maxRange ) {
				d.set(average);
			}
			context.write(groupedKey, d);
			i++;
		}
	}
}
