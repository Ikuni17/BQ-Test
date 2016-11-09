package bqTest;

import com.google.cloud.bigquery.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test {

    static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    static String projectId = "mitate-144222";
    static String datasetId = "test";
    static String test_metricdata = "test_metricdata";
    static DatasetId dsId = DatasetId.of(projectId, datasetId);
    static DatasetInfo ds = Dataset.of(dsId);
    static TableId tableId = TableId.of(datasetId, "test");
    static TableId logsTable = TableId.of(datasetId, "test_logs");
    static TableId metricDataTable = TableId.of(datasetId, "test_metricdata");
    static TableId transferExecutedByTable = TableId.of(datasetId, "test_transferexecutedby");
    static String name = "Aaa";
    static int id = 0001;
    static String name2 = "Bbb";
    static int id2 = 0002;
    static String field1 = "name";
    static String field2 = "id";
    static String[] metricDataFields = {"metricid", "transferid", "transactionid", "value", "transferfinished", "deviceid", "responsedata"};

    static Table table;


    public static void main(String... args) {


        // ITS WORKING!!
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
    }

    public static void metricDataTest() {
        //System.out.print(ds.getDatasetId());
        Table mtd = bigquery.getTable(datasetId, test_metricdata);

        //System.out.print(mtd);
        List<InsertAllRequest.RowToInsert> rows = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put(metricDataFields[0], 10001);
        row1.put(metricDataFields[1], 123456);
        row1.put(metricDataFields[2], 7890);
        row1.put(metricDataFields[3], 1.234);
        row1.put(metricDataFields[4], "yes?");
        row1.put(metricDataFields[5], "brad's phone");

        Map<String, Object> row2 = new HashMap<>();
        row2.put(metricDataFields[0], 10002);
        row2.put(metricDataFields[1], 789123);
        row2.put(metricDataFields[2], 4567);
        row2.put(metricDataFields[3], 5.678);
        row2.put(metricDataFields[4], "no");
        row2.put(metricDataFields[5], "mike's phone");

        rows.add(InsertAllRequest.RowToInsert.of(row1));
        rows.add(InsertAllRequest.RowToInsert.of(row2));

        InsertAllResponse insertResponse = mtd.insert(rows);

        if (insertResponse.hasErrors()) {
            System.out.println("Errors occurred while inserting rows");

        }


    }
}
