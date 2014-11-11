#!env php
<?php

use Aws\S3\S3Client;
use Aws\S3\Exception\S3Exception;

require 'vendor/autoload.php';

if (count($argv) <= 2) {
    print "Usage: php run.php <environment> <aqi-id>\n";
    print "Credentials stored in default profile in ~/.aws/credentials\n";
    print "Credentials documentation here: http://docs.aws.amazon.com/aws-sdk-php/guide/latest/credentials.html#credential-profiles\n";
    exit(1);
}

define('BUCKET', 'bc-store-sql-results');

$environment = $argv[1];
$id          = $argv[2];
$client      = S3Client::factory(array('profile' => 'default'));
$prefix      = "adhoc/{$environment}/{$id}/";
$iterator    = $client->getIterator('ListObjects', array('Bucket' => BUCKET, 'Prefix' => $prefix));
$first       = true;
$fh          = fopen("adhoc-{$environment}-{$id}.csv", 'w');
$count       = 76;
$index       = 0;

foreach ($iterator as $object) {
    $pathInfo = pathinfo($object['Key']);

    if ($pathInfo['extension'] != 'tsv') {
        continue;
    }

    $index++;

    print "Processing {$object['Key']} ({$index}/{$count})\n";

    $result   = $client->getObject(array('Bucket' => BUCKET, 'Key' => $object['Key']));
    $tsvData  = (string) $result['Body'];
    $tsvLines = explode("\n", $tsvData);

    foreach ($tsvLines as $lineNumber => $tsvLine) {
        if ($lineNumber == 0) {
            if ($first) {
                $tsvFields = explode("\t", $tsvLine);
                fputcsv($fh, $tsvFields);
                $first = false;
            }

            continue;
        }

        print "$tsvLine\n";

        $tsvFields = explode("\t", $tsvLine);
        fputcsv($fh, $tsvFields);
    }
}

fclose($fh);
