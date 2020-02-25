package parmanix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class EpsilonMapper implements MapperInterface<String, String, String, String> {

	@Override
	public String map(String product, String valueIn) {
		final String[] data = valueIn.trim().split(",");

		try {
//			final double price = Double.parseDouble(data[2]);
			return data[2];
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("Problematic line: ");
			System.out.println(product);
			System.out.println(valueIn);
			throw e;
		}
	}

	@Override
	public void configure(String[] paramArrayOfString) {
		// TODO Auto-generated method stub
		
	}
}
