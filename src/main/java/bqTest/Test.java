package bqTest;

import com.google.cloud.bigquery.*;
import com.google.gson.*;
import com.google.gson.stream.JsonWriter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONWriter;

import java.io.*;
import java.util.*;

public class Test {
    static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
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

    public static void main(String... args) throws IOException {
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
        logsTest();
        transferExecutedByTest();
        //metricDataTable.insert(metricRowsToInsert);
        //logsTable.insert(logsRowsToInsert);
        //transferExecutedByTable.insert(transferExecutedByRowsToInsert);

        //GsonBuilder builder = new GsonBuilder();
        //Gson gson = builder.create();
        //String sMetricRow = metricRowsToInsert.get(0).getContent().toString();

        JSONArray jArray = new JSONArray(metricRowsToInsert);
        //JsonArray gArray = new JsonArray();
        //JSONObject json = new JSONObject(metricRowsToInsert.get(0).getContent());
        //Iterator stuff = metricRowsToInsert.iterator();
        //JSONObject json2 = new JSONObject();

        //System.out.println(metricRowsToInsert.size());
        //while (stuff.hasNext()){
            //jArray.put(stuff.next());
            //json2.append();
            //System.out.println(stuff.next());
        //}
        //System.out.print("Json: ");
        //System.out.println(json);
        System.out.print("JArray: ");
        System.out.println(jArray);
        File file = new File("testJson.json");
        FileOutputStream out = new FileOutputStream(file);
        JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));
        writer.setIndent("\n");
        //writer.beginArray();
        //writer.
        //System.out.println(jArray.get(0).getClass());
        //System.out.println("String: " + sMetricRow);
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
        row1.put(metricDataFields[0], 10001);
        row1.put(metricDataFields[1], 123456);
        row1.put(metricDataFields[2], 7890);
        row1.put(metricDataFields[3], 1.234);
        row1.put(metricDataFields[4], "yes?");
        row1.put(metricDataFields[5], "json test");

        metricRowsToInsert.add(InsertAllRequest.RowToInsert.of(row1));
        row1 = new HashMap<>();

        row1.put(metricDataFields[0], 10001);
        row1.put(metricDataFields[1], 123456);
        row1.put(metricDataFields[2], 7890);
        row1.put(metricDataFields[3], 1.234);
        row1.put(metricDataFields[4], "yes?");
        row1.put(metricDataFields[5], "json2 test");

        metricRowsToInsert.add(InsertAllRequest.RowToInsert.of(row1));
    }
}
