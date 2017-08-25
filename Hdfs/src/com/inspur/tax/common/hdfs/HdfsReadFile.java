package com.inspur.tax.common.hdfs ;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inspur.tax.common.hdfs.HdfsUtils;

/**
 * 
 * @author Samza Jerry
 * @param
 * @return HdfsReadFile HdfsReadFile 2016年12月19日下午7:40:09
 */
class HdfsReadFile {

	
	private static final Logger logger = LoggerFactory.getLogger(HdfsReadFile.class);
	
	public static String uri = "hdfs://192.168.1.212:8020";
	public static String dir = "/user/WordInput";
	public String parentDir = "/user";

	public static void main(String args[]) {
		System.setProperty("hadoop.home.dir", "/Users/ruijia/Downloads/hadoop-2.6.0");
		logger.info("HdfsReadFile Starting...");

		String hdfsFile = testReadHDFSFile();
		// deal result

		String[] resultSetTmp = hdfsFile.split("\n");
		int row = resultSetTmp.length;
		System.out.println(row);
		String[][] resultSet = new String[10][10];
		for (int i = 0; i < row; i++) {

			String[] resultTmp = resultSetTmp[i].split(" ");
			for (int j = 0; j < resultTmp.length; j++) {
				resultSet[i][j] = resultTmp[j];
			}

		}

		for (int i = 0; i < row; i++) {
			for (int j = 0; j < 5; j++) {
				if (resultSet[i][j] != null) {
					System.out.println(resultSet[i][j]);
				}

			}
		}

		System.out.println("HdfsReadFile Ending...");

		System.out.println("HdfsFile2Oracle Starting...");

		HdfsReadFile.testOracle(resultSet, row);

		System.out.println("HdfsFile2Oracle Ending...");
	}

	public static String testReadHDFSFile() {

		String result = null;
		// HdfsUtils.testUploadLocalFile2HDFS();
		try {

			String remoteFile = dir + "/file.txt";
			String  hdfsFile = uri + remoteFile;  
	        Configuration conf = new Configuration();  
	        FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
			result = new String(HdfsUtils.readHDFSFile(remoteFile,fs));

		} catch (Exception e) {
			e.printStackTrace();

		}

		return result;
	}

	/**
	 * test oracle connecting
	 */

	public static void testOracle(String[][] values, int row) {
		ResultSet rs = null;
		Statement stmt = null;
		Connection conn = null;

		// deal fileResult

		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			// new oracle.jdbc.driver.OracleDriver();
			conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.227:1521:orcl", "metadb_v2", "My_678#");
			stmt = conn.createStatement();
			for (int i = 0; i < row; i++) {
				String name1 = values[i][0];
				String name2 = values[i][1];
				stmt.executeQuery("insert into hbase_meta (name1,name2)values(' " + name1 + " ', ' " + name2 + " ')");
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
					rs = null;
				}
				if (stmt != null) {
					stmt.close();
					stmt = null;
				}
				if (conn != null) {
					conn.close();
					conn = null;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
