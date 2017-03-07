import java.util.Arrays;
import java.util.stream.Stream;

import com.xavient.dip.samza.Utils.FlatJsonConverter;

public class TestJSON {

	public static void main(String[] args) {
		/*String feed = "{\"id\":\"XIND10000\",\"author\":\"Jack White\",\"title\":\"Hadoop Made Easy\",\"genre\":\"Programming\",\"price\":\"50000\",\"publish_date\":\"2001-09-10\",\"description\":\"ebook\"}";
		  
		Object[] feeds = FlatJsonConverter.convertToValuesArray(feed);
		Stream<Object> stream1 = Arrays.stream(feeds);
		
		//stream1.forEach(x -> System.out.println(x));

		StringBuilder data = new StringBuilder();
		
		stream1.forEach(x -> data.append("," +x));
		data.delete(0, 1);
		System.out.println(data.toString());
		*/
	System.out.println(String.valueOf(System.currentTimeMillis()));	
		
	}

}
