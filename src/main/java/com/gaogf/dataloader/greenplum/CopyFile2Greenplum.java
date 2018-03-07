package com.gaogf.dataloader.greenplum;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import scala.Array;
import scala.collection.JavaConversions;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pactera on 2018/3/7.
 */
public class CopyFile2Greenplum {
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL = "";
    private static final String USER = "";
    private static final String PASSWORD = "";
    public void copyFile(List<ArrayList<String>> data, String table){
        Connection connection = null;
        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            CopyManager cm = new CopyManager((BaseConnection)connection);
            long in = cm.copyIn("COPY" + table + "FROM STDIN", inputStream(data));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(connection != null){
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public InputStream inputStream(final List<ArrayList<String>> data){
        final PipedOutputStream out = new PipedOutputStream();
        final int rows = data.size();
        final int columns = data.get(0).size();
        (new Thread(){
            @Override
            public void run() {
                while (rows != 0){
                    for (int i = 0; i < rows; i++ ) {
                        for(int j = i; j < columns; j++){
                            try {
                                out.write((data.get(i).get(j) + (j == columns - 1?"\r\n":"\t")).getBytes());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        PipedInputStream inputStream = new PipedInputStream();
        try {
            inputStream.connect(out);
            return inputStream;
        } catch (IOException e) {
            e.printStackTrace();
            return inputStream;
        }
    }
}
