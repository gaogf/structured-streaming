package com.gaogf.dataloader.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by gaogf on 2018/3/7.
 */
public class BulkLoadFile {
    private static final Logger log = LoggerFactory.getLogger(BulkLoadFile.class);
    private JdbcTemplate jdbcTemplate;
    private Connection conn = null;

    public void setDataSource(DataSource dataSource){
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public int bulkloadFileInputStream(String sql, InputStream in){
        int result = 0;
        try {
            Connection connection = jdbcTemplate.getDataSource().getConnection();
            PreparedStatement statement = connection.prepareStatement(sql);
            if(statement.isWrapperFor(com.mysql.jdbc.Statement.class)){
                com.mysql.jdbc.PreparedStatement resultStatement = statement.unwrap(com.mysql.jdbc.PreparedStatement.class);
                resultStatement.setLocalInfileInputStream(in);
                result = resultStatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }
    public static void main(String args[]){
        String sql = "LOAD DATA LOCAL INFILE 'sql.csv' IGNORE INTO TABLE test.test (a,b,c,d,e,f)";
        try (InputStream in = new FileInputStream(new File(""))) {
            BulkLoadFile loadFile = new BulkLoadFile();
            long beginTime = System.currentTimeMillis();
            int rows = loadFile.bulkloadFileInputStream(sql, in);
            long endTime = System.currentTimeMillis();
            log.info("import " + rows + "rows data into mysql and cost " + (endTime - beginTime) + "ms!" );
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
