package com.amazonaws.samples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;
public class Main {
	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";

	public static Integer checkVersion(Statement stmt) throws SQLException {
		String sql = "SELECT version = CHANGE_TRACKING_CURRENT_VERSION ()";
		ResultSet rs = stmt.executeQuery(sql);
		Integer version = 0;
		while(rs.next()) {
			version = rs.getInt("version");
		}
		return version;

	}
	public static void sendMessageCustomerPay(Statement stmt, Integer oldVersionCustomerPay) throws SQLException, IOException {
	    if (!checkVersion(stmt).equals(oldVersionCustomerPay)) {
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
		    String fileName = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		    PrintWriter writer = new PrintWriter("./change-customer-pay/" + fileName + ".csv", "UTF-8");
		    PrintWriter writerVersion = new PrintWriter("./change-customer-pay/version.txt", "UTF-8");

		    while (rs.next()) {
				status = rs.getString("SYS_CHANGE_OPERATION");
			    writer.append(status);
			    writer.append(COMMA_DELIMITER);
			    writer.append(Integer.toString(rs.getInt("id")));

				if(!status.equals("D")) {
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("ACCOUNT_NUMBER"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("RECEIPTS_DATE"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("TANTOCD"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("BANK_CODE"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("BRANCH_CODE"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("AZUKARI_STATUS_ID"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("RECEIPTS_AMOUNT"));
				}
				writer.append(NEW_LINE_SEPARATOR);

	        }
		    writerVersion.println(Integer.toString(checkVersion(stmt)));
		    // write new version to file
		    writer.close();
		    writerVersion.close();
		    System.out.println("Write file CustomerPay");
	    }
	    System.out.print("No message!\n");
	}

	public static void sendMessageAzukariHistory(Statement stmt, Integer oldVersionAzukariHistory) throws SQLException, IOException {
	    if (!checkVersion(stmt).equals(oldVersionAzukariHistory)) {
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
		    String fileName = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		    PrintWriter writer = new PrintWriter("./change-azukari-history/" + fileName + ".csv", "UTF-8");
		    PrintWriter writerVersion = new PrintWriter("./change-azukari-history/version.txt", "UTF-8");
		    while (rs.next()) {
				status = rs.getString("SYS_CHANGE_OPERATION");
				writer.append(status);
			    writer.append(COMMA_DELIMITER);
			    writer.append(Integer.toString(rs.getInt("id")));
				if(!status.equals("D")) {
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("IR_FLG"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("ACTION_ID"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("FURIKAE_STATUS_ID"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("AZUKARI_STATUS_ID"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("R_YMD"));
					writer.append(COMMA_DELIMITER);
					writer.append("\"" + rs.getString("MEMO") + "\"");
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("R_ID"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("TANTOCD"));
					writer.append(COMMA_DELIMITER);
					writer.append(rs.getString("CUSTOMER_PAY_ID"));
				}
				writer.append(NEW_LINE_SEPARATOR);
	        }
		    writerVersion.println(Integer.toString(checkVersion(stmt)));
		    writer.close();
		    writerVersion.close();
		    System.out.println("Write file AzukariHistory");
	    }
	    System.out.print("No message!\n");
	}

	public static void sendMessageSeikyu(Statement stmt, Integer oldVersionSeikyu) throws SQLException, IOException {
	    if (!checkVersion(stmt).equals(oldVersionSeikyu)) {
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
		    String fileName = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		    PrintWriter writer = new PrintWriter("./change-seikyu/" + fileName + ".csv", "UTF-8");
		    PrintWriter writerVersion = new PrintWriter("./change-seikyu/version.txt", "UTF-8");

		    while (rs.next()) {
				status = rs.getString("SYS_CHANGE_OPERATION");
				writer.append(status);
			    writer.append(COMMA_DELIMITER);
			    writer.append(rs.getString("KAISYACD"));
			    writer.append(COMMA_DELIMITER);
			    writer.append(Integer.toString(rs.getInt("SEQNO")));
			    writer.append(COMMA_DELIMITER);
			    writer.append(Integer.toString(rs.getInt("GYONO")));
				if(!status.equals("D")) {
					writer.append(COMMA_DELIMITER);
				    writer.append(rs.getString("CUSTOMER_NAME"));
				    writer.append(COMMA_DELIMITER);
				    writer.append(rs.getString("SEIKYUGAKU"));
				}
	        }
		    writer.append(NEW_LINE_SEPARATOR);
		    writerVersion.println(Integer.toString(checkVersion(stmt)));
		    writer.close();
		    writerVersion.close();
		    System.out.println("Write file Seikyu");
	    }
	    System.out.print("No message!\n");
	}
	public static void sendMessageNyukin(Statement stmt, Integer oldVersionNyukin) throws SQLException, IOException {
	    Integer currentVersionNyukin = checkVersion(stmt);
	    if (!checkVersion(stmt).equals(oldVersionNyukin)) {
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
		    String fileName = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		    PrintWriter writer = new PrintWriter("./change-nyukin/" + fileName + ".csv", "UTF-8");
		    PrintWriter writerVersion = new PrintWriter("./change-nyukin/version.txt", "UTF-8");

		    while (rs.next()) {
				status = rs.getString("SYS_CHANGE_OPERATION");
				writer.append(status);
			    writer.append(COMMA_DELIMITER);
			    writer.append(rs.getString("KAISYACD"));
			    writer.append(COMMA_DELIMITER);
			    writer.append(Integer.toString(rs.getInt("SEQNO")));
			    writer.append(COMMA_DELIMITER);
			    writer.append(Integer.toString(rs.getInt("GYONO")));
				if(!status.equals("D")) {
					writer.append(COMMA_DELIMITER);
				    writer.append(rs.getString("KINGAKU"));
				    writer.append(COMMA_DELIMITER);
				    writer.append(rs.getString("AUTHENTIFICATION_CP_ID"));
				}
				writer.append(NEW_LINE_SEPARATOR);
	        }
		    System.out.println("Write file Nyukin");
		    writerVersion.println(Integer.toString(currentVersionNyukin));
		    writer.close();
		    writerVersion.close();
	    }
	    System.out.print("No message!\n");
	}

  public static Integer getVersion(String fileName) {
	  Integer version = 0;
	  try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) {
				version = Integer.parseInt(sCurrentLine);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	  return version;
  }
  public static void main(String[] args) {
	  String connectionUrl = "jdbc:sqlserver://localhost:11433;DatabaseName=VOneG3;user=Administrator;password=i04npsys";

	  TimerTask task = new TimerTask() {
		  @Override
		  public void run() {
			// Create a variable for the connection string.
		        try (Connection con = DriverManager.getConnection(connectionUrl); Statement stmt = con.createStatement();) {
		            // Iterate through the data in the result set and display it.
		            sendMessageCustomerPay(stmt, getVersion("./change-customer-pay/version.txt"));
		            sendMessageAzukariHistory(stmt, getVersion("./change-azukari-history/version.txt"));
		            sendMessageSeikyu(stmt, getVersion("./change-seikyu/version.txt"));
		            sendMessageNyukin(stmt, getVersion("./change-nyukin/version.txt"));
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
