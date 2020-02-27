package sample.controller;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import sample.common.Bigquery;

@RestController
public class HelloController {
	
	
	@RequestMapping("/")
	public String index() {
		
		return "";
	}
	
	@RequestMapping("/countHits")
	public String hello() throws JobException, InterruptedException {
		
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		StringBuilder sb = new StringBuilder();
		
		int len = 0;
		int i_len = 0;
		BigDecimal sum = BigDecimal.ZERO;
		
		String query = "SELECT '한화생명' AS NAME\r\n" + 
				"       , SUM(totals.hits) as count\r\n" + 
				"  FROM `hanwha-ga360.195231946.ga_sessions_*`\r\n" + 
				"UNION ALL\r\n" + 
				"SELECT '한화투자증권' AS NAME\r\n" + 
				"       , SUM(totals.hits) as count\r\n" + 
				"  FROM `hanwha-ga360.191298234.ga_sessions_*`\r\n" + 
				"";
		
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
		
		// Print the results.
		for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
			len++;
			i_len = 0;
		  for (FieldValue val : row) {
			
			if(i_len==0) {
				sb.append("채널 : ");
			}else {
				sb.append("숫자 : ");
				sum = sum.add(val.getNumericValue());
			}
			
		    sb.append(val.getValue());
		    
		    i_len++;
		  }
		  sb.append("<p>");
		}
		sb.append("<p>");
		sb.append("합계 : ");
		sb.append(sum);
		
		return sb.toString();
	}
	
	@RequestMapping("/bigQuery_test")
	public String bigQueryTest() {
		
		TableResult result2 = null;
		
		try {
			
			FileWriter fw = new FileWriter("C:\\Temp\\test.json");
			
			String datetime = "20200224";				// 쿼리 할 날짜
			String projectName = "hanwha-ga360"; // 해당 값은 프로젝트 예시 값
			String datasetName = "191298234";			// 해당 값은 데이터 셋 예시 값

			String query = "SELECT TO_JSON_STRING(T, true) as json_data FROM `" + projectName + "." + datasetName + ".ga_sessions_" + datetime + "` AS T";
//			query += " LIMIT 5";
			
			BigInteger bigInteger = BigInteger.ZERO;
			
			result2 = new Bigquery().getBigQuery(query);
			
			Long length 	= result2.getTotalRows();
			Long i 			= Long.valueOf("1");
			
			System.out.println("query : " + query);
			System.out.println("length : " + length);
			
//			System.out.println("SCHEMA [" + result2.getSchema() + "]");
//			System.out.println("VALUES [" + result2.getValues() + "]");
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			System.out.println(sdf.format(new Date()));
			
			fw.append("[\n");
			
			for(FieldValueList testF:result2.iterateAll()) {
				
				for(FieldValue val:testF) {
					fw.append(val.getStringValue());
				}
				
				if(i<length) {
					fw.append(",");
				}
				i++;
				
				System.out.println("i = " + i);
				
			}
			fw.append("\n]");
			fw.close();
			
/*			Iterator<FieldValueList> iter = result2.getValues().iterator();
			
			System.out.println("length : " + result2.getTotalRows());
												
			
			while(iter.hasNext()) {
				
				FieldValueList fvl = iter.next();
//				System.out.println(fvl.toString());
				
				/*Iterator<FieldValue> iter2 = fvl.iterator();
				
				while(iter2.hasNext()) {
					FieldValue fv = iter2.next();
					System.out.println(fv.getStringValue());
				}
			}*/
			
		} catch (Throwable e) {
			e.printStackTrace();
		}
		
		return result2.getValues().toString();
	}
	
	@RequestMapping("/json_test")
	public String jsonTest() throws IOException, ParseException {
		
		JSONParser parser = new JSONParser(); 
			
		Object obj = parser.parse(new FileReader("C:\\Temp\\test.json")); 
		
		JSONArray jsonA = (JSONArray) obj;
		
		
		Iterator iter = jsonA.iterator();
		
		while(iter.hasNext()) {
			
			JSONObject jo = (JSONObject) iter.next();
			
			System.out.println(jo.get("visitStartTime"));
		}
		
		System.out.println(jsonA.size());
		
		return jsonA.toJSONString();
	}
}
