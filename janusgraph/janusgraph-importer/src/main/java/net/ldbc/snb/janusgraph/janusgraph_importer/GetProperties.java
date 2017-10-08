package net.ldbc.snb.janusgraph.janusgraph_importer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class GetProperties {
	
	public List<String> getPropValues() throws IOException {
		
		InputStream inputStream = null;
		List<String> result = new ArrayList<>();

		try {
			Properties prop = new Properties();
			String propFileName = "config.properties";
			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

			if (inputStream != null)
			{
				prop.load(inputStream);
			}else
			{
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}

			// get the property value and print it out
			String hbaseConfFile = prop.getProperty("hbaseConfFile");
			String snbDataDir = prop.getProperty("snbDataDir");
			String batchSize = prop.getProperty("batchSize");
			String progReportPeriod = prop.getProperty("progReportPeriod");
			
			System.out.println(
					"hbaseConfFile=" + hbaseConfFile + "\n" +
					"snbDataDir=" + snbDataDir + "\n" +
					"batchSize=" + batchSize + "\n" +
					"progReportPeriod=" + progReportPeriod + "\n"
					);

			// add the property value to the result
			result.add(hbaseConfFile);
			result.add(snbDataDir);	
			result.add(batchSize);
			result.add(progReportPeriod);
		}catch(Exception e) 
		{
			e.printStackTrace();
		}finally
		{
			inputStream.close();
		}

		return result;
	}

}
