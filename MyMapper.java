package parmanix;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

//public class MyMapper {

//	public class MyMapper extends Object implements MapperInterface<String, String, Integer> {
//		  public String[][] map(String paramString) {
//		    String[][] arrayOfString = new String[1][2];
//		    arrayOfString[0][0] = paramString;
//		    arrayOfString[0][1] = "1";
//		    return arrayOfString;
//		  }
//		  
//		  public String getInputGroup(String paramString) { return "G"; }
//		  
//		  public String getOutputKey(String paramString) { return paramString; }
//		  
//		  public Integer getOutputVal(String paramString) { return Integer.valueOf(2); }
//		  
//		  public void configure(String[] paramArrayOfString) {}
//		}
public class MyMapper extends Mapper<String, String, String, Double> {
	  public void map(String line, String value, Context context   ) throws IOException, InterruptedException {
		  String[] splits = line.split("\t");
		  String productName = splits[0];
		  Double productCount = Double.parseDouble(splits[3]);
		  context.write(productName, productCount);
		}
	  
	  public String getInputGroup(String paramString) { return "G"; }
	  
	  public String getOutputKey(String paramString) { return paramString; }
	  
	  public Integer getOutputVal(String paramString) { return Integer.valueOf(2); }
	  
	  public void configure(String[] paramArrayOfString) {}
	}
//}
