package parmanix;

// 

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MapperWrapper extends Mapper<Object, Text, Text, DoubleWritable> {
//  static boolean JIKES = false;
	public static String getInputGroup(String line) {
		// TODO: Analyze the line and assign an actual group (Data Provider's job)
		return "G";
	}

	private static void enforceRange(Double minValue, Double maxValue, ArrayList<String[]> untrustedMapperResult)
			throws Exception {
		for (int i = 0; i < untrustedMapperResult.size(); i++) {
			String[] pair = untrustedMapperResult.get(i);
			String key = pair[0];
			Double value = Double.parseDouble(pair[1]);

			// Enforce the range
			Double minValDouble = minValue.doubleValue();
			Double maxValDouble = maxValue.doubleValue();

			if (minValDouble > maxValDouble) {
				throw new Exception("minValue needs to be smaller than maxValue");
			}
			Double mid = minValDouble + (minValDouble + maxValDouble) / 2.0;
			if (value < minValDouble || value > maxValDouble) {
				pair[1] = mid.toString();
//				untrustedMapperResult.set(i, Pair.create(key, mid.toString()));
			}
		}
	}

	public void map(Object someUnkownKey, org.w3c.dom.Text currentLine, Context context) throws IOException, InterruptedException {
		
		Configuration currentConfiguration = context.getConfiguration();
		// Custom map depending on dataset
		String dataset = currentConfiguration.get("dataset");
		ArrayList<String[]> kvPairs = new ArrayList<String[]>();
		// TODO: Here add custom dataset mappers
		if ( dataset.compareToIgnoreCase("Bigshop") == 0 ) {
			// TODO: Check p.7 that says each input should be mapped to a list of K,V pair
			kvPairs = getBigshopProductCountPair(currentLine.toString());	
			
		} else if (dataset.compareToIgnoreCase("EpsilonFull") == 0) {
			// TODO: Choose which columns we return to the untrusted mapper
			// Right now, we will provide full access
			kvPairs = getEpsilonFullPair(currentLine.toString());
		} else if (dataset.compareToIgnoreCase("EpsilonAnonymized") == 0) {
			// TODO: Choose which columns we return to the untrusted mapper
			// Right now, we will provide full access
			kvPairs = getEpsilonAnonymizedPairByProduct(currentLine.toString());
		} else if (dataset.compareToIgnoreCase("20newsgroup") == 0) {
			// TODO: Get the name of the document somewhere
			String fileName = "TODO";
			ArrayList<String[]> tokens = getNewsgroupProductCountPair(fileName, currentLine.toString());
			kvPairs.addAll(tokens);
		} else {
			// TODO: Deal correctly with this
			System.out.println("Configuration for dataset not supported: " + dataset);
		}
		
		// TODO: Output key type is here set to some arbitrary value, but this should be
		// given
		// by Computation Provider (or maybe Data Provider, not sure)
		String lineGroup = MapperWrapper.getInputGroup(currentLine.toString());
//	        if (JIKES)
//	          DIFC.startMapInvocation(b);
		// TODO: Change this to the real untrusted mapper via another mechanism to be found
		ArrayList<String[]> untrustedMapperResult = new ArrayList();
		String mapperClassName = currentConfiguration.get("untrustedMapperClassName");
		try {
			MapperInterface untrustedMapper = (MapperInterface) Class.forName(mapperClassName).getConstructor().newInstance();
			// This next lines passes to the untrusted mapper a key of NULL and a value containing ALL the Key-Value pairs as read from  the dataset
//			untrustedMapperResult = (Map<String, Double>) um.map(null, kvPairs);
			// Or we can map explicitly:
			// In the weird case that the Mapper creates multiple records, we will iterate over the result:
			for (String[] pair : kvPairs) {
				String key = pair[0];
				String result = (String) untrustedMapper.map(key, pair[1]);
				
				String[] newKv = new String[2];
				newKv[0] = key;
				newKv[1] = result;
				untrustedMapperResult.add(newKv);
			}
		} catch (InstantiationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalArgumentException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvocationTargetException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchMethodException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
//	        if (JIKES)
//	          DIFC.endMapInvocation(); 

		Configuration configuration = context.getConfiguration();
		Double minValue = Double.parseDouble(configuration.get("minRange"));
		Double maxValue = Double.parseDouble(configuration.get("maxRange"));
		try {
			enforceRange(minValue, maxValue, untrustedMapperResult);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DoubleWritable valueWritable = new DoubleWritable();
		for (String[] pair: untrustedMapperResult) {
			String key = pair[0];
			Double doubleValue = Double.parseDouble(pair[1]);
			// TODO: We also need to change the key to have the group information 
			Text newKey = new Text(key.concat("_").concat(lineGroup));
			// Write output to Hadoop (gets transmitted to the reducers)
			valueWritable.set(doubleValue);
			try {
				context.write(newKey, valueWritable);
			} catch (Exception e) {
				System.out.println("Error in writing to context. Key is " + newKey + " value is " + valueWritable);
				e.printStackTrace();
			}
			
		}
	}

	private static ArrayList<String[]> getBigshopProductCountPair(String inputLine) {
		ArrayList<String[]> result = new ArrayList();
		String[] splits = inputLine.split("\t");
		String productName = splits[0];
		String productCount = splits[3];
		
		String[] pair = new String[2];
		pair[0] = productName;
		pair[1] = productCount;
		result.add(pair);
		// TODO Auto-generated method stub
		return result;
	}
	
	// This is the Dataset Provider method to extract information from a dataset
	// so that it is available to the untrusted mapper
	private static ArrayList<String[]> getEpsilonFullPair(String inputLine) {
		ArrayList<String[]> result = new ArrayList();	
		String[] splits = inputLine.split(",");	
		String[] pair = new String[2];
		if (!splits[0].equals("Transaction_date")) {
			pair[0] = splits[1];
			pair[1] = inputLine;
			result.add(pair);			
		}
		
		// TODO Auto-generated method stub
		return result;
	}
	
	// This is the Dataset Provider method to extract information from a dataset
	// so that it is available to the untrusted mapper
	private static ArrayList<String[]> getEpsilonProductPair(String inputLine) {
		ArrayList<String[]> result = new ArrayList();
		String[] splits = inputLine.split(",");
		String productName = splits[1];
		String productPrice = splits[2];
		
		String[] pair = new String[2];
		pair[0] = productName;
		pair[1] = productPrice;
		result.add(pair);
		// TODO Auto-generated method stub
		return result;
	}
	
	// This is the Dataset Provider method to extract information from a dataset
	// so that it is available to the untrusted mapper
	// This needs to return ["ProductName", stringContainingOnlyNonConfidentialData]
	private static ArrayList<String[]> getEpsilonAnonymizedPairByProduct(String inputLine) {
		// You receive externally (maybe as runtime parameters the key the queryer wants
		// and you analyze if you can actually emit that parameter as a key
		// (you won't let someone use 'Name' as a key. In this case we just use Product as a key
		ArrayList<String[]> result = new ArrayList();
		String[] splits = inputLine.split(",");
		StringBuilder sb = new StringBuilder();
		// This only contains ['Transaction_date', 'Product', 'Price', 'Payment_Type', 'City', 'State', 'Country']
		sb.append(splits[0]).append(",").append(splits[1]).append(",").append(splits[2]).append(",").append(splits[3]).append(",").append(splits[5]).append(",").append(splits[6]).append(",").append(splits[7]).append(",");
		String[] pair = new String[2];
		pair[0] = splits[1];
		pair[1] = sb.toString();
		result.add(pair);
		// TODO Auto-generated method stub
		return result;
	}
	
	// This is the Dataset Provider method to extract information from a dataset
	// so that it is available to the untrusted
	// Check if we should just read the file instead.
	
	private static ArrayList<String[]> getNewsgroupProductCountPair(String fileName, String textOnOneLine) {
		ArrayList<String[]> result = new ArrayList();
		String[] words = textOnOneLine.split(" ");
		for (String word: words) {
			String[] pair = new String[2];
			String key = fileName + '_' + word;
			pair[0] = key;
			pair[1] = word;
			result.add(pair);
		}
		
		return result;
	}
}

//
