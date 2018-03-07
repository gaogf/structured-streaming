package com.gaogf.dataloader.mysql;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Created by pactera on 2018/3/6.
 */
public class JdbcUtils {
    private SaveMode mode = SaveMode.Append;
    private static String driver = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://localhost:3306/gaogf";
    private PreparedStatement statement;
    private static Connection conn = null;

    private static Connection createConnection(){
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url);
            return conn;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    public static boolean tableExists(String tableName){
        boolean b = false;
        try {
            b = conn.prepareStatement("SELECT 1 FROM " + tableName + "LIMIT 1")
                    .executeQuery().next();
            return b;
        } catch (SQLException e) {
            e.printStackTrace();
            return b;
        }
    }
    public static void dropTable(String tableName){
        try {
            conn.prepareStatement("DROP TABLE " + tableName).executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static PreparedStatement insertStatement(Connection connection,String tableName, StructType structType){
        String[] fieldNames = structType.fieldNames();
        StringBuilder builder = new StringBuilder();
        int i = 0;
        builder.append("(");
        for (String field: fieldNames) {
            builder.append(field);
            if(i == fieldNames.length-1){
                builder.append(")");
            }else{
                builder.append(",");
            }
            i++;
        }
        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName);
        sql.append(builder);
        sql.append(" VALUES (");
        int index = fieldNames.length;
        while(index > 0){
            sql.append("?");
            if(index > 1){
                sql.append(",");
            }else{
                sql.append(")");
            }
            index--;
        }
        try {
            return connection.prepareStatement(sql.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    public static void savePartition(Connection connection, String tableName, StructType structType, Iterator<Row> iterator, ArrayList<Integer> nullType){
        connection = createConnection();
        PreparedStatement preparedStatement = null;
        boolean commited = false;
        try {
            connection.setAutoCommit(false);
            preparedStatement = insertStatement(connection, tableName, structType);
            while(iterator.hasNext()){
                Row row = iterator.next();
                int fieldsNum = structType.fieldNames().length;
                int i = 0;
                while(i < fieldsNum){
                    if(row.isNullAt(i)){
                        preparedStatement.setNull(i + 1,nullType.get(i));
                    }else {
                        switch (structType.fields()[i].dataType().toString()){
                            case "IntegerType":
                                preparedStatement.setInt(i + 1,row.getInt(i));
                                break;
                            case "LongType":
                                preparedStatement.setLong(i + 1,row.getLong(i));
                                break;
                            case "DoubleType":
                                preparedStatement.setDouble(i + 1,row.getDouble(i));
                                break;
                            case "FloatType":
                                preparedStatement.setFloat(i + 1, row.getFloat(i));
                                break;
                            case "ShortType":
                                preparedStatement.setInt(i + 1, row.getShort(i));
                                break;
                            case "ByteType":
                                preparedStatement.setInt(i + 1, row.getByte(i));
                                break;
                            case "BooleanType":
                                preparedStatement.setBoolean(i + 1, row.getBoolean(i));
                                break;
                            case "StringType":
                                preparedStatement.setString(i + 1, row.getString(i));
                            case "BinaryType":
                                preparedStatement.setBytes(i + 1, row.getAs(i));
                                break;
                            case "TimestampType":
                                preparedStatement.setTimestamp(i + 1, row.getAs(i));
                                break;
                            case "DateType":
                                preparedStatement.setDate(i + 1, row.getAs(i));
                                break;
                            case "DecimalType":
                                preparedStatement.setBigDecimal(i + 1, row.getDecimal(i));
                                break;
                            default:
                                throw new IllegalArgumentException("Can't translate non-null value for field " + i);
                        }
                    }
                    i++;
                }
                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                conn.commit();
                commited = true;
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                if(!commited){
                    try {
                        conn.rollback();
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }finally {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
    public static final Map<DataType,String> typeMap = new ConcurrentHashMap<>();
    static {
        typeMap.put(IntegerType,"INTEGER");
        typeMap.put(LongType,"BIGINT");
        typeMap.put(DoubleType,"DOUBLE PRECISION");
        typeMap.put(FloatType,"REAL");
        typeMap.put(ShortType,"INTEGER");
        typeMap.put(ByteType,"BYTE");
        typeMap.put(BooleanType,"BIT(1)");
        typeMap.put(StringType,"TEXT");
        typeMap.put(BinaryType,"BLOB");
        typeMap.put(TimestampType,"TIMESTAMP");
        typeMap.put(DateType,"DATE");
    }

}
