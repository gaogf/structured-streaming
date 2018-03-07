package com.gaogf.structuredstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Created by pactera on 2018/3/7.
 */
public class SocketWordCount {
    public static void main(String args[]){
        SparkSession spark = SparkSession.builder().appName("SocketWordCount")
                .master("local[2]")
                .getOrCreate();
        Dataset<Row> dataset = spark.readStream().format("socket")
                .option("host", "localhost")
                .option("port", "9990")
                .load();
        dataset.isStreaming();
        StructType structType = new StructType()
                .add("name", "string")
                .add("age", "integer");
        Dataset<Row> csv = spark.readStream().option("sep", ";")
                .schema(structType)
                .csv("path/to/csv/file");
    }
}
