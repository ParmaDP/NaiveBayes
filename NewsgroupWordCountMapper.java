package parmanix;

import java.util.ArrayList;

public class NewsgroupWordCountMapper implements MapperInterface<String, String, String, Double> {

	// TODO: Determine type of IT. Either type is "Text" (from Hadoop) or can be a String
	// that contains the whole text.
	@Override
	public Double map(String keyIn, String valueIn) {
		// TODO: We receive tokens with key being the documentId_word and value being the word
		return 1.0D;
	}

	@Override
	public void configure(String[] paramArrayOfString) {
		// TODO Auto-generated method stub
		
	}

}
