package com.imooc.jdbc.test;

import com.imooc.jdbc.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class BatchTest {

    @Test
    public void batchTest() {
        Connection connection = null;
        PreparedStatement properties = null;
        try {
            connection = JdbcUtil.getConnection();
            JdbcUtil.begin(connection);
            String sql = "INSERT INTO person (username, email, gender, dept_id) VALUES (?,?,?,?)";
            properties = connection.prepareStatement(sql);

            long beginTime = System.currentTimeMillis();

            for (int i = 0; i < 10000; i++) {
                properties.setString(1, "hello" + (i+1));
                properties.setString(2, "world" + (i+1));
                properties.setString(3, "gender" + (i+1));
                properties.setInt(4, i + 1);
                properties.executeUpdate();
            }

            JdbcUtil.commit(connection);    //  1075

            long endTime = System.currentTimeMillis();

            System.out.println("totalTime: " + (endTime - beginTime));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void batchTwoTest() {
        Connection connection = null;
        PreparedStatement properties = null;

        try {
            connection = JdbcUtil.getConnection();
            JdbcUtil.begin(connection);

            String sql = "INSERT INTO person (username, email, gender, dept_id) VALUES (?,?,?,?)";
            properties = connection.prepareStatement(sql);

            for (int i = 10000; i < 20000; i++) {
                properties.setString(1, "hello" + (i+1));
                properties.setString(2, "world" + (i+1));
                properties.setString(3, "gender" + (i+1));
                properties.setInt(4, i + 1);
                properties.addBatch();

                if ((i + 1) % 500 == 0) {
                    properties.executeBatch();
                    properties.clearBatch();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
