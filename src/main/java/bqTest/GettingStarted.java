// This is a program to test the Google BQ API for authentication and a basic query
// TODO test insert into a sample database for MITATE integration

package bqTest;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Example of authorizing with Bigquery and reading from a public dataset.
 *
 * Specifically, this queries the shakespeare dataset to fetch the 10 of Shakespeare's works with
 * the greatest number of distinct words.
 */
public class GettingStarted {
    // [START build_service]
    /**
     * Creates an authorized Bigquery client service using Application Default Credentials.
     *
     * @return an authorized Bigquery client
     * @throws IOException if there's an error getting the default credentials.
     */
    public static Bigquery createAuthorizedClient() throws IOException {
        // Create the credential
        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);

        // Depending on the environment that provides the default credentials (e.g. Compute Engine, App
        // Engine), the credentials may require us to specify the scopes we need explicitly.
        // Check for this case, and inject the Bigquery scope if required.
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }

        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("Bigquery Samples")
                .build();
    }
    // [END build_service]

    // [START run_query]
    /**
     * Executes the given query synchronously.
     *
     * @param querySql the query to execute.
     * @param bigquery the Bigquery service object.
     * @param projectId the id of the project under which to run the query.
     * @return a list of the results of the query.
     * @throws IOException if there's an error communicating with the API.
     */
    private static List<TableRow> executeQuery(String querySql, Bigquery bigquery, String projectId)
            throws IOException {
        QueryResponse query =
                bigquery.jobs().query(projectId, new QueryRequest().setQuery(querySql)).execute();

        // Execute it
        GetQueryResultsResponse queryResult =
                bigquery
                        .jobs()
                        .getQueryResults(
                                query.getJobReference().getProjectId(), query.getJobReference().getJobId())
                        .execute();

        return queryResult.getRows();
    }
    // [END run_query]

    // [START print_results]
    /**
     * Prints the results to standard out.
     *
     * @param rows the rows to print.
     */
    private static void printResults(List<TableRow> rows) {
        System.out.print("\nQuery Results:\n------------\n");
        for (TableRow row : rows) {
            for (TableCell field : row.getF()) {
                System.out.printf("%-50s", field.getV());
            }
            System.out.println();
        }
    }
    // [END print_results]

    /**
     * Exercises the methods defined in this class.
     *
     * In particular, it creates an authorized Bigquery service object using Application Default
     * Credentials, then executes a query against the public Shakespeare dataset and prints out the
     * results.
     *
     * @param args the first argument, if it exists, should be the id of the project to run the test
     *     under. If no arguments are given, it will prompt for it.
     * @throws IOException if there's an error communicating with the API.
     */
    public static void listDatasets(final Bigquery bigquery, final String projectId)
            throws IOException {
        Bigquery.Datasets.List datasetRequest = bigquery.datasets().list(projectId);
        DatasetList datasetList = datasetRequest.execute();
        if (datasetList.getDatasets() != null) {
            List<DatasetList.Datasets> datasets = datasetList.getDatasets();
            System.out.println("Available datasets\n----------------");
            System.out.println(datasets.toString());
            for (DatasetList.Datasets dataset : datasets) {
                System.out.format("%s\n", dataset.getDatasetReference().getDatasetId());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        /*Scanner sc;
        if (args.length == 0) {
            // Prompt the user to enter the id of the project to run the queries under
            System.out.print("Enter the project ID: ");
            sc = new Scanner(System.in);
        } else {
            sc = new Scanner(args[0]);
        }
        String projectId = sc.nextLine();*/

        String projectId = "mitate-144222";
        String datasetId = "test";
        String tableId = "test";
        String name = "Hillary";
        int id = 1111;
        String name2 = "Chris";
        int id2 = 1212;

        // Create a new Bigquery client authorized via Application Default Credentials.
        Bigquery bigquery = createAuthorizedClient();
        //listDatasets(bigquery, projectId);

        TableRow data = new TableRow();
        data.set("name", name);
        data.set("id", new Integer(id));
        TableRow data2 = new TableRow();
        data2.set("name", name2);
        data2.set("id", new Integer(id2));

        TableDataInsertAllRequest.Rows content = new TableDataInsertAllRequest.Rows();
        content.setInsertId(String.valueOf(System.currentTimeMillis()));
        content.setJson(data);
        TableDataInsertAllRequest request = new TableDataInsertAllRequest();
        request.setRows(Arrays.asList(content));

        TableDataInsertAllResponse response = bigquery.tabledata().insertAll(projectId, datasetId, tableId, request).execute();
        System.out.print(response.getInsertErrors());
        List<TableRow> rows =
                executeQuery(
                        "SELECT name, id FROM [mitate-144222:test.test] ORDER BY name ASC",
                        bigquery,
                        projectId);

        printResults(rows);
    }
}
// [END all]