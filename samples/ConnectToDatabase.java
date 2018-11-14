/**
 * 
 */
package com.amazonaws.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * @author v.lai
 *
 */
public class ConnectToDatabase {
	private static final String QUEUE_NAME = "supersonic";
	private static Integer currentVersion = 1;
	private static Integer oldVersion = currentVersion;

	public static Integer checkVersion(Statement stmt) throws SQLException {
		String sql = "SELECT version = CHANGE_TRACKING_CURRENT_VERSION ()";
		ResultSet rs = stmt.executeQuery(sql);
		Integer version = 0;
		while(rs.next()) {
			version = rs.getInt("version");
		}
		return version;	
		
	}
	
	public static void sendMessage(Statement stmt) throws SQLException {
		final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

	    String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
	    System.out.print("Connected AWS SQS!!!\n");
	    currentVersion = checkVersion(stmt);
	    if (currentVersion != oldVersion) {
	    	// Change Tracking
	        String SQL = "DECLARE @PreviousVersion bigint\n" + 
	        		"SET @PreviousVersion = " + Integer.toString(oldVersion) + "\n" + 
	        		"SELECT CTTable.ID, CTTable.SYS_CHANGE_OPERATION, \n" + 
	        		"Emp.name, Emp.age, \n" + 
	        		"CTTable.SYS_CHANGE_VERSION \n" + 
	        		"FROM CHANGETABLE (CHANGES TableTest, @PreviousVersion) AS CTTable\n" + 
	        		"LEFT OUTER JOIN TableTest AS Emp\n" + 
	        		"ON emp.ID = CTTable.ID\n" ;
		    ResultSet rs = stmt.executeQuery(SQL);
		    Integer count = 0; //count number of message
		    while (rs.next()) {
		    	count++;
		    	Map<String, com.amazonaws.services.sqs.model.MessageAttributeValue> messageAttributes = new HashMap<>();
			    messageAttributes.put("ID", new MessageAttributeValue()
			            .withDataType("Number")
			            .withStringValue(Integer.toString(rs.getInt("id"))));
			    messageAttributes.put("SYS_CHANGE_OPERATION", new MessageAttributeValue()
			            .withDataType("String")
			            .withStringValue(rs.getString("SYS_CHANGE_OPERATION")));
			    messageAttributes.put("NAME", new MessageAttributeValue()
			            .withDataType("String")
			            .withStringValue(rs.getString("name")));
			    messageAttributes.put("AGE", new MessageAttributeValue()
			            .withDataType("Number")
			            .withStringValue(rs.getString("age")));
			    SendMessageRequest sendMessageRequest = new SendMessageRequest();
			    sendMessageRequest.withMessageBody("Change Tracking");
			    sendMessageRequest.withQueueUrl(queueUrl);
			    sendMessageRequest.withMessageAttributes(messageAttributes);
//			    sendMessageRequest.setMessageGroupId("messageGroup" + Integer.toString(count));
//			    sendMessageRequest.setMessageDeduplicationId(Integer.toString(count));
			    sqs.sendMessage(sendMessageRequest);
	        }
		    System.out.print("Send messages!!!\n");
		    oldVersion = currentVersion;
	    }
	}
	public static void main(String[] args) {
		// Create a variable for the connection string.
        String connectionUrl = "jdbc:sqlserver://localhost:11433;DatabaseName=VOneG3;user=Administrator;password=i04npsys";

        try (Connection con = DriverManager.getConnection(connectionUrl); Statement stmt = con.createStatement();) {
            // Iterate through the data in the result set and display it.
        	System.out.print("Connected database!!!\n");
            sendMessage(stmt);
        }
        // Handle any errors that may have occurred.
        catch (SQLException e) {
            e.printStackTrace();
        }
	}
}
