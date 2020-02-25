package parmanix;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;

public class TrustedReducers {
	public static Double sumReducer(String group, String key, Iterable<DoubleWritable> values) {
		Double sum = 0.0D;
		for (DoubleWritable v : values) {
			sum += v.get();
		}
		return sum;
	}
	
	public static Double maxReducer(String group, String key, Iterable<DoubleWritable> values) {
		Double minimum = null;
		for (DoubleWritable v : values) {
			Double currentValue = v.get();
			if (minimum == null || currentValue > minimum)
				minimum = currentValue;
		}
		return minimum;
	}
	
	public static  Double minReducer(String group, String key, Iterable<DoubleWritable> values) {
		Double minimum = null;
		for (DoubleWritable v : values) {
			Double currentValue = v.get();
			if (minimum == null || currentValue < minimum)
				minimum = currentValue;
		}
		return minimum;
	}
	
	public static Double meanReducer(String group, String key, Iterable<DoubleWritable> values) {
		Double sum = 0.0D;
		int i = 0;
		for (DoubleWritable v : values) {
			sum += v.get();
			i++;
		}
		return sum / i;
	}
	
	public static Double countReducer(String group, String key, Iterable<DoubleWritable> values) {
		Double i = 0.0D;
		for (DoubleWritable v : values) {
			i++;
		}
		return i;
	}
	
	public static Double medianReducer(String group, String key, Iterable<DoubleWritable> values) {
		Double sum = 0.0D;
		int i = 0;
		List<Double> target = new ArrayList<>();
		for (DoubleWritable v : values) {
			target.add(v.get());
		}
		Collections.sort(target);
		int middle = target.size() / 2;
       
        if (target.size() % 2 == 1) {
            return target.get(middle);
        } else {
           return (target.get(middle-1) + target.get(middle)) / 2.0;
        }
	}
}
