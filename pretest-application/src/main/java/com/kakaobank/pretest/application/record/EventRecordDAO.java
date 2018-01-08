package com.kakaobank.pretest.application.record;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class EventRecordDAO {
    private static final Logger logger = Logger.getLogger(EventRecordDAO.class.getName());

    final Properties mysqlProps;
    final String insert = "insert into events(event_id, event_timestamp, service_code, event_context) values(?,?,?,?)";

    public EventRecordDAO(Properties mysqlProps) {
        this.mysqlProps = mysqlProps;
    }

    public int add(EventRecord record) {
        logger.debug("insert single record : " + record);
        int resultCount = 0;

        try ( Connection c = this.createConnection();
              PreparedStatement ps = c.prepareStatement(insert)
        ) {
            ps.setLong(1, record.getEventID());
            ps.setString(2, record.getEventTimestamp());
            ps.setString(3, record.getServiceCode());
            ps.setString(4, record.getEventContext());

            resultCount = ps.executeUpdate();
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("event message create connection faile. ", e);
            e.printStackTrace();
        }

        logger.info("insert record complete : " + resultCount);
        return resultCount;
    }

    public int[] add(List<EventRecord> records) throws SQLException, ClassNotFoundException {
        logger.info("insert records : " + records.size());

        Connection c = this.createConnection();
        PreparedStatement ps = c.prepareStatement(insert);

        c.setAutoCommit(false);

        for(EventRecord record : records) {
            ps.setLong(1, record.getEventID());
            ps.setString(2, record.getEventTimestamp());
            ps.setString(3, record.getServiceCode());
            ps.setString(4, record.getEventContext());
            ps.addBatch();
        }

        return ps.executeBatch();
    }

    public List<EventRecord> get(long event_id, String event_timestamp) throws ClassNotFoundException, SQLException {
        List<EventRecord> readRecords = new LinkedList<>();

        Connection c = this.createConnection();
        PreparedStatement ps = c.prepareStatement(insert);

        ps.setLong(1, event_id);
        ps.setString(2, event_timestamp);

        ResultSet rs = ps.executeQuery();

        while(rs.isLast() == false) {
            rs.next();
            readRecords.add(new EventRecord(rs.getLong("event_id"),
                    rs.getString("event_timestamp"),
                    rs.getString("service_code"),
                    rs.getString("event_context")));
        }

        return readRecords;
    }

    private Connection createConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection(
                mysqlProps.getProperty("url") + "/" + mysqlProps.getProperty("db"),
                mysqlProps.getProperty("user"),
                mysqlProps.getProperty("password"));
    }
}
