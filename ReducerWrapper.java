package parmanix;


import java.io.BufferedReader;
import parmanix.*;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.math3.analysis.function.Max;
import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.slf4j.Logger;


// TODO: Get the range from somewhere
public class ReducerWrapper extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	static String UNIQUE_GROUP = "G";
	private Double minRange;
	private Double maxRange;
	private Double estimatedSensitivity;

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Begin reduce");
		Configuration configuration = context.getConfiguration();
		Double epsilon = Double.parseDouble(configuration.get("epsilon"));
		this.minRange = Double.parseDouble(configuration.get("minRange"));
		this.maxRange = Double.parseDouble(configuration.get("maxRange"));
		System.out.println("Using minRange value " + minRange + " maxRange value " + maxRange);
		double average = minRange + (maxRange - minRange) / 2.0D;
		double delta = Double.parseDouble(configuration.get("delta"));
		int N = Integer.parseInt(configuration.get("n"));
		this.estimatedSensitivity = 1.0D;
		String[] splits = key.toString().split("_");
		String realKey = splits[0];
		String group = splits[1];
		Double trustedReducerResult = null;
		// Read configuration to get name of reducer class to use.
		String reducerOperation = configuration.get("trustedReducerName");
		if ( reducerOperation.compareToIgnoreCase("sum") == 0 ) {
			// This formula only applies if minRange is 0
			// this.estimatedSensitivity = Math.max(Math.abs(minRange), Math.abs(maxRange));
			// More accurate formula:
			this.estimatedSensitivity = Math.abs(maxRange - minRange);
			trustedReducerResult = TrustedReducers.sumReducer(group, realKey, values);
		} else if (reducerOperation.compareToIgnoreCase("min") == 0 ) {
			// TODO: Change estimatedSensitivity
			trustedReducerResult = TrustedReducers.minReducer(group, realKey, values);
		} else if (reducerOperation.compareToIgnoreCase("max") == 0 ) {
			// TODO: Change estimatedSensitivity
			trustedReducerResult = TrustedReducers.maxReducer(group, realKey, values);
		} else if (reducerOperation.compareToIgnoreCase("mean") == 0 ) {
			// TODO: Check a way to find this
			double degreeOfParallelism = 1.0D;
			this.estimatedSensitivity = Math.abs(maxRange - minRange) / degreeOfParallelism;
			// TODO: Check for real formula of mean sensitivity (depends on number of mappers)
			trustedReducerResult = TrustedReducers.meanReducer(group, realKey, values);
		} else if (reducerOperation.compareToIgnoreCase("median") == 0 ) {
			// TODO: Change estimatedSensitivity
			this.estimatedSensitivity = average;
			trustedReducerResult = TrustedReducers.medianReducer(group, realKey, values);
		} else if (reducerOperation.compareToIgnoreCase("count") == 0 ) {
			this.estimatedSensitivity = 1.0D;
			trustedReducerResult = TrustedReducers.countReducer(group, realKey, values);
		} else if (reducerOperation.compareToIgnoreCase("identity") == 0) {
			// TODO: This Needs to be removed in the real Airavat implementation as it is possible
			// to output the real values by constructing a key in the untrustedMapper with all the values
			// of the record.
			for (DoubleWritable d: values) {
				context.write(key, d);
			}
		} else {
			// TODO: Deal correctly with this
			System.out.println("Reducer " + reducerOperation + " is not valid. Please choose a reducer from the list: [sum, min, max, mean, median, count]");
			trustedReducerResult = -1.0D;
		}
		
		// We range enforce AND enforce something with regards to the Count
		if (trustedReducerResult < minRange || trustedReducerResult > maxRange) {
			System.out.println(realKey + " Result " + trustedReducerResult + " is out of range, clamping to " + average);
			trustedReducerResult = average;
		}
		// Add some noise
		trustedReducerResult += getNoise(epsilon, delta);
		DoubleWritable finalResult = new DoubleWritable(trustedReducerResult);
		Text t = new Text(realKey);
		context.write(t, finalResult);

	}

	public double getNoise(double epsilon, double delta) {
		// Since our reducer wrapper is currently a sum, it's sensitivity is the max
		// absolute value the expected output
		// (the omission of a single record can at most deviate of a value of this
		// value)
		System.out.println("Standard deviation is " + this.estimatedSensitivity / epsilon);
		 LaplaceDistribution laplaceDist = new LaplaceDistribution(0.0D, this.estimatedSensitivity / epsilon);
		 return laplaceDist.sample();
	//   	return 0.0D;
	}
}

//}

