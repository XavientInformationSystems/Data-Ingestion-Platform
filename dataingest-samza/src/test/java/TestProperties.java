import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;




public class TestProperties {

	public static void main(String[] args) throws IOException {
		ClassLoader loader = Thread.currentThread().getContextClassLoader(); 
		InputStream resourceStream = loader.getResourceAsStream("job.properties"); 
		Properties prop = new Properties();
		
		prop.load(resourceStream);
	 System.out.println(prop.getProperty("systems.hdfsDump.samza.factory"));
	}
}
