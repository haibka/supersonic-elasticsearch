package com.amazonaws.samples;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
public class Main {
	private static final String QUEUE_NAME = "supersonic";
	private static Integer currentVersion = 0;
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
	public static void sendMessage(Statement stmt) throws SQLException, IOException {
		final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

	    String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
//	    System.out.print("Connected AWS SQS!!!\n");
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
		    String status;
		    Integer count = 0;
		    List<SendMessageBatchRequestEntry> entries = new LinkedList<>();
		    SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();

		    while (rs.next()) {
		    	count++;
				status = rs.getString("SYS_CHANGE_OPERATION");
				Map<String, com.amazonaws.services.sqs.model.MessageAttributeValue> messageAttributes = new HashMap<>();
				messageAttributes.put("SYS_CHANGE_OPERATION", new MessageAttributeValue()
				        .withDataType("String")
				        .withStringValue(status));
				messageAttributes.put("ID", new MessageAttributeValue()
				        .withDataType("Number")
				        .withStringValue(Integer.toString(rs.getInt("id"))));
				if(!status.equals("D")) {
					System.out.print(status);
				    messageAttributes.put("NAME", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("name")));
				    messageAttributes.put("AGE", new MessageAttributeValue()
				            .withDataType("Number")
				            .withStringValue(rs.getString("age")));
				}
				entry.setMessageBody("Change Tracking");
				entry.setMessageAttributes(messageAttributes);
				entry.setId(Integer.toString(count));
				if (count > 10) {
				    sqs.sendMessageBatch(queueUrl, entries);
				    while (!entries.isEmpty()) {
				        entries.remove(0);
				    }
				    count = 0;
				    entries.add(entry);
				    entry = new SendMessageBatchRequestEntry();
				    System.out.println("Sent Message!!!");
				}
				else {
				    entries.add(entry);
				    entry = new SendMessageBatchRequestEntry();
				}

//			    sendMessageRequest.setMessageGroupId("messageGroup" + Integer.toString(count));
//			    sendMessageRequest.setMessageDeduplicationId(Integer.toString(count));
			    oldVersion = currentVersion;
	        }
		    if ( count > 0) {
			    sqs.sendMessageBatch(queueUrl, entries);
			    System.out.println("Sent Message!!!");
			    while (!entries.isEmpty()) {
			        entries.remove(0);
			    }
			}
	    }
	    System.out.print("No message!\n");
	}
  public static void main(String[] args) {
	  String connectionUrl = "jdbc:sqlserver://localhost:11433;DatabaseName=VOneG3;user=Administrator;password=i04npsys";
	  TimerTask task = new TimerTask() {
		  @Override
		  public void run() {
			// Create a variable for the connection string.
		        try (Connection con = DriverManager.getConnection(connectionUrl); Statement stmt = con.createStatement();) {
		            // Iterate through the data in the result set and display it.
//		        	System.out.print("Connected database!!!\n");
		            sendMessage(stmt);
		            con.close();
		            stmt.close();
		        }
		        // Handle any errors that may have occurred.
		        catch (SQLException e) {
		            e.printStackTrace();
		        } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			  }
	  };
	  Timer timer = new Timer();
	  long delay = 0;
	  long intevalPeriod = 10000;
	  // schedules the task to be run in an interval (ms)
	  timer.scheduleAtFixedRate(task, delay,
			  intevalPeriod);
	  } // end of main
  }
