<?php
require 'vendor/autoload.php';

// Returns Mitate BQ and test dataset
list($bigQuery, $dataset) = require 'get_bq_connection.php';
$table = $dataset->table('test');

if(!$bigQuery){
    echo 'No BQ!';
}

// Insertion test
$insertResponse = $table->insertRow(['name' => 'PHPTest', 'id' => 101]);
if (!$insertResponse->isSuccessful()) {
    $row = $insertResponse->failedRows()[0];

    print_r($row['rowData']);

    foreach ($row['errors'] as $error) {
        echo $error['reason'] . ': ' . $error['message'] . PHP_EOL;
    }
}

$queryResults = $bigQuery->runQuery('SELECT name, id FROM [mitate-144222:test.test] ORDER BY name ASC');

$isComplete = $queryResults->isComplete();

while (!$isComplete) {
    sleep(1);
    $queryResults->reload();
    $isComplete = $queryResults->isComplete();
}

echo 'Query complete!';

foreach ($queryResults->rows() as $row) {
    print_r($row);
}

?>