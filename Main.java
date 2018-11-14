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
	private static Integer currentVersionCustomerPay = 0;
	private static Integer currentVersionAzukariHistory = 0;
	private static Integer currentVersionSeikyu = 0;
	private static Integer currentVersionNyukin = 0;
	private static Integer oldVersionCustomerPay = currentVersionCustomerPay;
	private static Integer oldVersionAzukariHistory = currentVersionAzukariHistory;
	private static Integer oldVersionSeikyu = currentVersionSeikyu;
	private static Integer oldVersionNyukin = currentVersionNyukin;

	public static Integer checkVersion(Statement stmt) throws SQLException {
		String sql = "SELECT version = CHANGE_TRACKING_CURRENT_VERSION ()";
		ResultSet rs = stmt.executeQuery(sql);
		Integer version = 0;
		while(rs.next()) {
			version = rs.getInt("version");
		}
		return version;

	}
	public static void sendMessageCustomerPay(Statement stmt) throws SQLException, IOException {
		final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

	    String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
//	    System.out.print("Connected AWS SQS!!!\n");
	    currentVersionCustomerPay = checkVersion(stmt);
	    if (currentVersionCustomerPay != oldVersionCustomerPay) {
	    	// Change Tracking
	        String SQL = "DECLARE @PreviousVersion bigint\n" +
	        		"SET @PreviousVersion = " + Integer.toString(oldVersionCustomerPay) + "\n" +
	        		"SELECT CTTable.ID, CTTable.SYS_CHANGE_OPERATION, CTTable.SYS_CHANGE_VERSION,\n" +
	        		"Cp.ACCOUNT_NUMBER, Cp.RECEIPTS_DATE, Cp.TANTOCD, Cp.BANK_CODE,\n" +
	        		"Cp.BRANCH_CODE, Cp.TRANSFER_NAME, Cp.IR_FLG, Cp.ACTION_ID, Cp.FURIKAE_STATUS_ID,\n" +
	        		"Cp.AZUKARI_STATUS_ID, Cp.RECEIPTS_AMOUNT\n" +
	        		"FROM CHANGETABLE (CHANGES TBLCUSTOMER_PAY, @PreviousVersion) AS CTTable\n" +
	        		"LEFT OUTER JOIN TBLCUSTOMER_PAY AS Cp\n" +
	        		"ON Cp.ID = CTTable.ID" ;
		    ResultSet rs = stmt.executeQuery(SQL);
		    String status;
		    Integer count = 1;
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
				    messageAttributes.put("ACCOUNT_NUMBER", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("ACCOUNT_NUMBER")));
				    messageAttributes.put("RECEIPTS_DATE", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("RECEIPTS_DATE")));
				    messageAttributes.put("TANTOCD", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("TANTOCD")));
				    messageAttributes.put("BANK_CODE", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("BANK_CODE")));
				    messageAttributes.put("BRANCH_CODE", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("BRANCH_CODE")));
				    messageAttributes.put("AZUKARI_STATUS_ID", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("AZUKARI_STATUS_ID")));
				    messageAttributes.put("RECEIPTS_AMOUNT", new MessageAttributeValue()
				            .withDataType("Number")
				            .withStringValue(rs.getString("RECEIPTS_AMOUNT")));
				}
				entry.setMessageBody("CUSTOMER_PAY");
				entry.setMessageAttributes(messageAttributes);
				entry.setId(Integer.toString(count));
				if (count > 10) {
				    sqs.sendMessageBatch(queueUrl, entries);
				    while (!entries.isEmpty()) {
				        entries.remove(0);
				    }
				    count = 1;
				    entries.add(entry);
				    entry = new SendMessageBatchRequestEntry();
				    System.out.println("Sent Message customer pay!!!");
				}
				else {
				    entries.add(entry);
				    entry = new SendMessageBatchRequestEntry();
				}

//			    sendMessageRequest.setMessageGroupId("messageGroup" + Integer.toString(count));
//			    sendMessageRequest.setMessageDeduplicationId(Integer.toString(count));
			    oldVersionCustomerPay = currentVersionCustomerPay;
	        }
		    if ( count > 1) {
			    sqs.sendMessageBatch(queueUrl, entries);
			    System.out.println("Sent Message customer pay!!!");
			    while (!entries.isEmpty()) {
			        entries.remove(0);
			    }
			}
	    }
	    System.out.print("No message!\n");
	}

	public static void sendMessageAzukariHistory(Statement stmt) throws SQLException, IOException {
		final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

	    String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
//	    System.out.print("Connected AWS SQS!!!\n");
	    currentVersionAzukariHistory = checkVersion(stmt);
	    if (currentVersionAzukariHistory != oldVersionAzukariHistory) {
	    	// Change Tracking
	        String SQL = "DECLARE @PreviousVersion bigint\n" +
	        		"SET @PreviousVersion = " + Integer.toString(oldVersionAzukariHistory) + "\n" +
	        		"SELECT CTTable.ID, CTTable.SYS_CHANGE_OPERATION, CTTable.SYS_CHANGE_VERSION,\n" +
	        		"AZ.CUSTOMER_PAY_ID, AZ.IR_FLG, AZ.ACTION_ID, AZ.FURIKAE_STATUS_ID, \n" +
	        		"AZ.AZUKARI_STATUS_ID, AZ.R_YMD, AZ.MEMO, AZ.R_ID, AZ.TANTOCD\n" +
	        		"FROM CHANGETABLE (CHANGES TBLAZUKARI_HISTORY, @PreviousVersion) AS CTTable\n" +
	        		"LEFT OUTER JOIN TBLAZUKARI_HISTORY AS AZ\n" +
	        		"ON AZ.ID = CTTable.ID" ;
		    ResultSet rs = stmt.executeQuery(SQL);
		    String status;
		    Integer count = 1;
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
				    messageAttributes.put("IR_FLG", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("IR_FLG")));
				    messageAttributes.put("ACTION_ID", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("ACTION_ID")));
				    messageAttributes.put("FURIKAE_STATUS_ID", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("FURIKAE_STATUS_ID")));
				    messageAttributes.put("AZUKARI_STATUS_ID", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("AZUKARI_STATUS_ID")));
				    messageAttributes.put("R_YMD", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("R_YMD")));
				    messageAttributes.put("MEMO", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("MEMO")));
				    messageAttributes.put("R_ID", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("R_ID")));
				    messageAttributes.put("TANTOCD", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("TANTOCD")));
				    messageAttributes.put("CUSTOMER_PAY_ID", new MessageAttributeValue()
				            .withDataType("Number")
				            .withStringValue(rs.getString("CUSTOMER_PAY_ID")));
				}
				entry.setMessageBody("AZUKARI_HISTORY");
				entry.setMessageAttributes(messageAttributes);
				entry.setId(Integer.toString(count));
				if (count > 10) {
				    sqs.sendMessageBatch(queueUrl, entries);
				    while (!entries.isEmpty()) {
				        entries.remove(0);
				    }
				    count = 1;
				    entries.add(entry);
				    entry = new SendMessageBatchRequestEntry();
				    System.out.println("Sent Message azukari history!!!");
				}
				else {
				    entries.add(entry);
				    entry = new SendMessageBatchRequestEntry();
				}

//			    sendMessageRequest.setMessageGroupId("messageGroup" + Integer.toString(count));
//			    sendMessageRequest.setMessageDeduplicationId(Integer.toString(count));
			    oldVersionAzukariHistory = currentVersionAzukariHistory;
	        }
		    if ( count > 1) {
			    sqs.sendMessageBatch(queueUrl, entries);
			    System.out.println("Sent Message azukari history!!!");
			    while (!entries.isEmpty()) {
			        entries.remove(0);
			    }
			}
	    }
	    System.out.print("No message!\n");
	}

	public static void sendMessageSeikyu(Statement stmt) throws SQLException, IOException {
		final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

	    String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
//	    System.out.print("Connected AWS SQS!!!\n");
	    currentVersionSeikyu = checkVersion(stmt);
	    if (currentVersionSeikyu != oldVersionSeikyu) {
	    	// Change Tracking
	        String SQL = "DECLARE @PreviousVersion bigint\n" +
	        		"SET @PreviousVersion = " + Integer.toString(oldVersionSeikyu) + "\n" +
	        		"SELECT CTTable.KAISYACD, CTTable.SEQNO, CTTable.GYONO, CTTable.SYS_CHANGE_OPERATION, CTTable.SYS_CHANGE_VERSION,\n" +
	        		"SK.CUSTOMER_NAME, SK.SEIKYUGAKU\n" +
	        		"FROM CHANGETABLE (CHANGES TBLSEIKYU, @PreviousVersion) AS CTTable\n" +
	        		"LEFT OUTER JOIN TBLSEIKYU AS SK\n" +
	        		"ON SK.KAISYACD = CTTable.KAISYACD AND SK.SEQNO = CTTable.SEQNO AND SK.GYONO = CTTable.GYONO" ;
		    ResultSet rs = stmt.executeQuery(SQL);
		    String status;
		    Integer count = 1;
		    List<SendMessageBatchRequestEntry> entries = new LinkedList<>();
		    SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();

		    while (rs.next()) {
		    	count++;
				status = rs.getString("SYS_CHANGE_OPERATION");
				Map<String, com.amazonaws.services.sqs.model.MessageAttributeValue> messageAttributes = new HashMap<>();
				messageAttributes.put("SYS_CHANGE_OPERATION", new MessageAttributeValue()
				        .withDataType("String")
				        .withStringValue(status));
				messageAttributes.put("KAISYACD", new MessageAttributeValue()
				        .withDataType("String")
				        .withStringValue("KAISYACD"));
				messageAttributes.put("SEQNO", new MessageAttributeValue()
				        .withDataType("Number")
				        .withStringValue(Integer.toString(rs.getInt("SEQNO"))));
				messageAttributes.put("GYONO", new MessageAttributeValue()
				        .withDataType("Number")
				        .withStringValue(Integer.toString(rs.getInt("GYONO"))));
				if(!status.equals("D")) {
				    messageAttributes.put("CUSTOMER_NAME", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("CUSTOMER_NAME")));
				    messageAttributes.put("SEIKYUGAKU", new MessageAttributeValue()
				            .withDataType("Number")
				            .withStringValue(rs.getString("SEIKYUGAKU")));
				}
				entry.setMessageBody("SEIKYU");
				entry.setMessageAttributes(messageAttributes);
				entry.setId(Integer.toString(count));
				if (count > 10) {
				    sqs.sendMessageBatch(queueUrl, entries);
				    while (!entries.isEmpty()) {
				        entries.remove(0);
				    }
				    count = 1;
				    entries.add(entry);
				    entry = new SendMessageBatchRequestEntry();
				    System.out.println("Sent Message seikyu!!!");
				}
				else {
				    entries.add(entry);
				    entry = new SendMessageBatchRequestEntry();
				}

//			    sendMessageRequest.setMessageGroupId("messageGroup" + Integer.toString(count));
//			    sendMessageRequest.setMessageDeduplicationId(Integer.toString(count));
			    oldVersionSeikyu = currentVersionSeikyu;
	        }
		    if ( count > 1) {
			    sqs.sendMessageBatch(queueUrl, entries);
			    System.out.println("Sent Message seikyu!!!");
			    while (!entries.isEmpty()) {
			        entries.remove(0);
			    }
			}
	    }
	    System.out.print("No message!\n");
	}
	public static void sendMessageNyukin(Statement stmt) throws SQLException, IOException {
		final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

	    String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
//	    System.out.print("Connected AWS SQS!!!\n");
	    currentVersionNyukin = checkVersion(stmt);
	    if (currentVersionNyukin != oldVersionNyukin) {
	    	// Change Tracking
	        String SQL = "DECLARE @PreviousVersion bigint\n" +
	        		"SET @PreviousVersion = " + Integer.toString(oldVersionNyukin) + "\n" +
	        		"SELECT CTTable.KAISYACD, CTTable.SEQNO, CTTable.GYONO, CTTable.SYS_CHANGE_OPERATION, CTTable.SYS_CHANGE_VERSION,\n" +
	        		"NK.KINGAKU, NK.AUTHENTIFICATION_CP_ID\n" +
	        		"FROM CHANGETABLE (CHANGES TBLNYUKIN, @PreviousVersion) AS CTTable\n" +
	        		"LEFT OUTER JOIN TBLNYUKIN AS NK\n" +
	        		"ON NK.KAISYACD = CTTable.KAISYACD AND NK.SEQNO = CTTable.SEQNO AND NK.GYONO = CTTable.GYONO" ;
		    ResultSet rs = stmt.executeQuery(SQL);
		    String status;
		    Integer count = 1;
		    List<SendMessageBatchRequestEntry> entries = new LinkedList<>();
		    SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();

		    while (rs.next()) {
		    	count++;
				status = rs.getString("SYS_CHANGE_OPERATION");
				Map<String, com.amazonaws.services.sqs.model.MessageAttributeValue> messageAttributes = new HashMap<>();
				messageAttributes.put("SYS_CHANGE_OPERATION", new MessageAttributeValue()
				        .withDataType("String")
				        .withStringValue(status));
				messageAttributes.put("KAISYACD", new MessageAttributeValue()
				        .withDataType("String")
				        .withStringValue("KAISYACD"));
				messageAttributes.put("SEQNO", new MessageAttributeValue()
				        .withDataType("Number")
				        .withStringValue(Integer.toString(rs.getInt("SEQNO"))));
				messageAttributes.put("GYONO", new MessageAttributeValue()
				        .withDataType("Number")
				        .withStringValue(Integer.toString(rs.getInt("GYONO"))));
				if(!status.equals("D")) {
				    messageAttributes.put("KINGAKU", new MessageAttributeValue()
				            .withDataType("String")
				            .withStringValue(rs.getString("KINGAKU")));
				    messageAttributes.put("AUTHENTIFICATION_CP_ID", new MessageAttributeValue()
				            .withDataType("Number")
				            .withStringValue(rs.getString("AUTHENTIFICATION_CP_ID")));
				}
				entry.setMessageBody("NYUKIN");
				entry.setMessageAttributes(messageAttributes);
				entry.setId(Integer.toString(count));
				if (count > 10) {
				    sqs.sendMessageBatch(queueUrl, entries);
				    while (!entries.isEmpty()) {
				        entries.remove(0);
				    }
				    count = 1;
				    entries.add(entry);
				    entry = new SendMessageBatchRequestEntry();
				    System.out.println("Sent Message nyukin!!!");
				}
				else {
				    entries.add(entry);
				    entry = new SendMessageBatchRequestEntry();
				}

//			    sendMessageRequest.setMessageGroupId("messageGroup" + Integer.toString(count));
//			    sendMessageRequest.setMessageDeduplicationId(Integer.toString(count));
			    oldVersionNyukin = currentVersionNyukin;
	        }
		    if (count > 1) {
			    sqs.sendMessageBatch(queueUrl, entries);
			    System.out.println("Sent Message nyukin!!!");
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
		            sendMessageCustomerPay(stmt);
		            sendMessageAzukariHistory(stmt);
		            sendMessageSeikyu(stmt);
		            sendMessageNyukin(stmt);
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
