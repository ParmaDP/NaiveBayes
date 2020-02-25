package parmanix;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

// Object, Text, Text, DoubleWritable
// GT, IT, KT, VT
public class IdentityMapper implements MapperInterface<String, String, Object, String> {

	@Override
	public String map(String keyIn, String valueIn) {
		// TODO: Check for accurate
		// Malicious code:
		// Write valueIn to local file
		return valueIn;
	}

	@Override
	public void configure(String[] paramArrayOfString) {
		// TODO Auto-generated method stub
	}

}
