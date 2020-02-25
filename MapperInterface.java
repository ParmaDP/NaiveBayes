package parmanix;

// VT (return of map) should return key and value
public interface MapperInterface<GT, IT, KT, VT> {
	  VT map(GT keyIn, IT valueIn);
//	  
//	  GT getInputGroup(String paramString);
//	  
//	  KT getOutputKey(String paramString);
//	  
//	  VT getOutputVal(String paramString);
	  
	  void configure(String[] paramArrayOfString);
	}
//
