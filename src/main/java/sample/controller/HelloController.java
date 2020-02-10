package sample.controller;

import java.util.Iterator;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;

@RestController
public class HelloController {
	
	@RequestMapping("/")
	public String hello() throws JobException, InterruptedException {
		
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		
		
		StringBuilder sb = new StringBuilder();
		
		int len = 0;
		
		String query = "SELECT visitorId, visitNumber, visitId, visitStartTime, date"
						+ "     FROM `hanwha-ga360.191298234.ga_sessions_20200203` limit 10";
		
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

		// Print the results.
		for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
			len++;
			
		  for (FieldValue val : row) {			  			  
			  
		    System.out.printf("%s,", val.toString());
		    sb.append(val.toString()).append("\n");
		  }
		  System.out.printf("\n");
		}
		
		System.out.println("length = " + len);
		
		return sb.toString();
	
	}
}
