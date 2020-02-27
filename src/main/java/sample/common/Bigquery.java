package sample.common;

import java.util.UUID;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;

public class Bigquery {
	
	//Call BigQuery
	//Parm : Query
	//Return : TableResult
	public static TableResult getBigQuery(String query) throws Throwable {

	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
	    // Use standard SQL syntax for queries.
	    // See: https://cloud.google.com/bigquery/sql-reference/

	    // Create a job ID so that we can safely retry.
	    JobId jobId = JobId.of(UUID.randomUUID().toString());
	    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

	    // Wait for the query to complete.
	    queryJob = queryJob.waitFor();
	    // Check for errors
	    if (queryJob == null) {
	      throw new RuntimeException("Job no longer exists");
	    } else if (queryJob.getStatus().getError() != null) {
	      // You can also look at queryJob.getStatus().getExecutionErrors() for all
	      // errors, not just the latest one.
	      throw new RuntimeException(queryJob.getStatus().getError().toString());
	    }

	    // Get the results.
	    QueryResponse response = bigquery.getQueryResults(jobId);
	    TableResult result = queryJob.getQueryResults();

	    return result;
	  }

}
