package net.mpolonioli.ldbcimpls.incubator.rya.interactive;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.json.JSONArray;
import org.json.JSONObject;

public class RyaClient {

	// example: "http://siti-rack.siti.disco.unimib.it:8080/web.rya/queryrdf"
	private String baseQueryUrl;
	
	// example: "http://siti-rack.siti.disco.unimib.it:8080/web.rya/loadrdf"
	private String baseLoadRdfUrl;
	
	public RyaClient(String baseQueryUrl, String baseLoadRdfUrl) {
		this.baseQueryUrl = baseQueryUrl + "?query.resultformat=json&query=";
		this.baseLoadRdfUrl = baseLoadRdfUrl;
	}
	
	public JSONArray executeReadQuery(String query) {
		
		JSONArray jsonBindings;
		
		try {
			String queryenc = URLEncoder.encode(query, "UTF-8");

			URL url = new URL(baseQueryUrl + queryenc);
			URLConnection urlConnection = url.openConnection();
			urlConnection.setDoOutput(true);

			BufferedReader rd = new BufferedReader(new InputStreamReader(
					urlConnection.getInputStream()));
			
            String line;
            String stringResult = "";
            while ((line = rd.readLine()) != null) {
                stringResult += line;
            }
            
    		JSONObject jsonObject = new JSONObject(stringResult);
    		JSONObject jsonResult = jsonObject.getJSONObject("results");
    		jsonBindings = jsonResult.getJSONArray("bindings");
            
            rd.close();
    		return jsonBindings;
    		
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}	
	}
	
	public void executeUpdateQuery(String query) {
		
		try {
			String queryenc = URLEncoder.encode(query, "UTF-8");

			URL url = new URL(baseQueryUrl + queryenc);
			URLConnection urlConnection = url.openConnection();
			urlConnection.getInputStream();
    		
		} catch(Exception e) {
			e.printStackTrace();
		}	
	}
	
	public void loadData(String rdf_data) {
		try {
			final InputStream resourceAsStream = new ByteArrayInputStream(rdf_data.getBytes(StandardCharsets.UTF_8));
			URL url = new URL(baseLoadRdfUrl +
					"?format=TURTLE" +
					"");
			URLConnection urlConnection = url.openConnection();
			urlConnection.setRequestProperty("Content-Type", "text/plain");
			urlConnection.setDoOutput(true);

			final OutputStream os = urlConnection.getOutputStream();

			int read;
			while((read = resourceAsStream.read()) >= 0) {
				os.write(read);
			}
			resourceAsStream.close();
			os.flush();

			BufferedReader rd = new BufferedReader(new InputStreamReader(
					urlConnection.getInputStream()));
			
			rd.close();
			os.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

