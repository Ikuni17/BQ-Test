package bqTest;

//stuff

import com.google.cloud.bigquery.*;
import com.google.gson.*;
import com.google.gson.stream.JsonWriter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONWriter;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class Test {
    static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    // Another way to instantiate BQ service
    /*static BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
    static BigQuery bigquery = optionsBuilder.build().getService();*/

    static String datasetId = "test";
    static String sMetricData = "test_metricdata";
    static String sLogs = "test_logs";
    static String sTransferExecutedBy = "test_transferexecutedby";

    static String[] metricDataFields = {"metricid", "transferid", "transactionid", "value", "transferfinished", "deviceid", "responsedata"};
    static String[] logsFields = {"logid", "username", "transferid", "deviceid", "logmessage", "transferfinished"};
    static String[] transferExececutedByFields = {"transferid", "devicename", "username", "carriername", "deviceid"};

    static Table metricDataTable = bigquery.getTable(datasetId, sMetricData);
    static Table logsTable = bigquery.getTable(datasetId, sLogs);
    static Table transferExecutedByTable = bigquery.getTable(datasetId, sTransferExecutedBy);

    static List<InsertAllRequest.RowToInsert> metricRowsToInsert = new ArrayList<>();
    static List<InsertAllRequest.RowToInsert> logsRowsToInsert = new ArrayList<>();
    static List<InsertAllRequest.RowToInsert> transferExecutedByRowsToInsert = new ArrayList<>();

    public static void main(String... args) throws IOException, TimeoutException, InterruptedException {
        // Different version of insertion
        /*Map<String, Object> firstRow = new HashMap<>();
        Map<String, Object> secondRow = new HashMap<>();
        firstRow.put(field1, name);
        firstRow.put(field2, id);
        secondRow.put(field1, name2);
        secondRow.put(field2, id2);

        InsertAllRequest insertRequest =
                InsertAllRequest.newBuilder(tableId).addRow(firstRow).addRow(secondRow).build();
        InsertAllResponse insertResponse = bigquery.insertAll(insertRequest);
        // Check if errors occurred
        if (insertResponse.hasErrors()) {
            System.out.println("Errors occurred while inserting rows");
        }*/

        metricDataTest();
        //logsTest();
        //transferExecutedByTest();

        //System.out.println(metricRowsToInsert);
        //metricRowsToInsert.clear();
        //System.out.println(metricRowsToInsert);

        InsertAllResponse metricResponse = metricDataTable.insert(metricRowsToInsert);
        System.out.println(metricResponse);
        System.out.println(metricResponse.getInsertErrors());
        if(metricResponse.hasErrors()){
            System.out.println("Has errors");
        }
        //logsTable.insert(logsRowsToInsert);
        //transferExecutedByTable.insert(transferExecutedByRowsToInsert);

        // This will take a set of rows and write to a JSON file
        // Another option is to use an iterator
        /*JSONArray jArray = new JSONArray(metricRowsToInsert);
        String filePath = "testJson.json";
        File file = new File(filePath);
        Gson gson = new Gson();
        String sJson = gson.toJson(jArray);
        FileWriter writer = new FileWriter(file);
        writer.write(sJson);
        writer.close();*/

    }

    public static void logsTest() {
        Map<String, Object> row1 = new HashMap<>();
        row1.put(logsFields[1], "channing");
        row1.put(logsFields[2], 123456);
        row1.put(logsFields[3], "galaxy s8");
        row1.put(logsFields[4], "dsakdjaskljdalk");
        row1.put(logsFields[5], "yes?");

        logsRowsToInsert.add(InsertAllRequest.RowToInsert.of(row1));
    }

    public static void transferExecutedByTest() {
        Map<String, Object> row1 = new HashMap<>();
        row1.put(transferExececutedByFields[0], 456789);
        row1.put(transferExececutedByFields[1], "galaxy s8");
        row1.put(transferExececutedByFields[2], "sam");
        row1.put(transferExececutedByFields[3], "verizon");
        row1.put(transferExececutedByFields[4], "adsadasd");

        transferExecutedByRowsToInsert.add(InsertAllRequest.RowToInsert.of(row1));
    }

    public static void metricDataTest() {
        Map<String, Object> row1 = new HashMap<>();
        row1.put(metricDataFields[0], 10003);
        row1.put(metricDataFields[1], 9876);
        row1.put(metricDataFields[2], 54321);
        row1.put(metricDataFields[3], 1.111);
        row1.put(metricDataFields[4], "hai");
        row1.put(metricDataFields[5], "jar test 1");

        metricRowsToInsert.add(InsertAllRequest.RowToInsert.of(row1));
        row1 = new HashMap<>();

        row1.put(metricDataFields[0], 10003);
        row1.put(metricDataFields[1], 123456);
        row1.put(metricDataFields[2], 7890);
        row1.put(metricDataFields[3], 1.234);
        row1.put(metricDataFields[4], "iie");
        row1.put(metricDataFields[5], "jar test 2");

        metricRowsToInsert.add(InsertAllRequest.RowToInsert.of(row1));
    }
}
