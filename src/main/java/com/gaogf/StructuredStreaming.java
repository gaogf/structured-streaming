package com.gaogf;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;

/**
 * Created by pactera on 2018/3/3.
 */
public class StructuredStreaming {
    public static void main(String args[]) {


        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("StructuredStreaming")
                .getOrCreate();
       /* spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent event) {
                System.out.println("1111111111111111111111111111" + event.id());
            }

            @Override
            public void onQueryProgress(QueryProgressEvent event) {
                System.out.println("2222222222222222222" + event.progress());
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent event) {
                System.out.println("333333333333333333" + event.id());
            }
        });*/
        Dataset<String> dataset = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9990")
                .load()
                .as(Encoders.STRING());
        Dataset<String> map = dataset.flatMap((FlatMapFunction<String, String>) f -> Arrays.asList(f.split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> count = map.groupBy("value").count().alias("counts");
        StreamingQuery start = count.writeStream().outputMode("complete").format("foreach").foreach(new ForeachWriter<Row>() {
            String driver = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://localhost:3306/gaogf";
            String username = "root";
            String password = "123456";
            Connection conn = null;
            PreparedStatement statement = null;
            @Override
            public void process(Row value) {
                StructType schema = value.schema();
                String[] fieldNames = schema.fieldNames();
                for (String field: fieldNames) {
                    System.out.println(field);
                }
                System.out.println(schema);
                try {
                    String sql = "insert into words VALUES (?,?)";
                    PreparedStatement prepareStatement = conn.prepareStatement(sql);
                    prepareStatement.setString(1,value.get(0).toString());
                    prepareStatement.setLong(2,Long.parseLong(value.get(1).toString()));
                    prepareStatement.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void close(Throwable errorOrNull) {
                try {
                    if(statement != null)
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                try {
                    if (conn != null)
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public boolean open(long partitionId, long version) {
                try {
                    Class.forName(driver);
                    conn = DriverManager.getConnection(url, username, password);
                    return true;
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    return false;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }).start();
        System.out.println("4444444444444444" + start.id());
        try {
            start.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }
}
