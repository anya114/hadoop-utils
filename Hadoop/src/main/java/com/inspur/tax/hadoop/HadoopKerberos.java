package com.inspur.tax.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inspur.tax.common.hdfs.HdfsUtils;
import com.inspur.tax.connectpool.ConnPool;
import com.inspur.tax.hadoop.ReadPropertyUtil;

/**
 * HDFS Kerberos 上传及目录展示
 * 
 * @author jerry.R
 * @return HadoopKerberos84s 2017年2月8日下午5:08:22
 */
public class HadoopKerberos {

	private static final Logger logger = LoggerFactory.getLogger(HadoopKerberos.class);

	public static void main(String[] args) {

		try {

			FileSystem fs = init();

			/**
			 * 测试IO流上传本地文件到HDFS
			 */
			logger.info("开始上传文件流...");
			/*
			 * FileInputStream localFileStream = new
			 * FileInputStream("/Users/ruijia/Desktop/123.html"); String
			 * hdfsFileOutPath = "/test_hadoop2/testUpload.html";
			 * HdfsUtils.uploadFileStream2HDFS(localFileStream, hdfsFileOutPath,
			 * fs);
			 */

			logger.info("上传成功...");

			logger.info("开始创建目录...");
			// HdfsUtils.mkdir("/mkdir_new", fs);
			logger.info("成功创建目录 ...");

			logger.info("开始读取...");
			HdfsUtils.printlist(fs, "/calculate/job");

			/*
			 * String FileRead =
			 * "/calculate/job/calculateService_257e1a28-38a6-44b4-8531-70333574e0d3.jar";
			 * byte[] byteResult = HdfsUtils.readHDFSFile(FileRead, fs);
			 * System.out.println(byteResult.length);
			 */

			logger.info("读取完毕...");

			logger.info("开始删除...");

			logger.info("删除成功...");

			logger.info("开始移动文件...");
			/*
			 * String hdfsFileBefore="/test_hadoop2/kerberos.java"; String
			 * hdfsFileAfter="/test_afterMove/133.java";
			 * HdfsUtils.moveFileOnHDFS(hdfsFileBefore, hdfsFileAfter, fs);
			 */
			logger.info("移动文件成功...");

			HdfsUtils.closeFileSystem(fs);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
		}
	}

	public static FileSystem init() throws IOException {
		System.clearProperty("java.security.krb5.conf");
		// 设置HDAOOP_HOME 环境变量
		// System.setProperty("hadoop.home.dir",
		// "/Users/ruijia/Downloads/hadoop-2.6.0");

		Configuration conf = ReadPropertyUtil.getConfiguration();

		boolean isUseKerberos = Boolean.valueOf(ReadPropertyUtil.readProperty().getProperty("isUseKerberos"));

		if (isUseKerberos) {
			String hdfsuserInfo = ReadPropertyUtil.readProperty().getProperty("hdfsuserInfo");

			String krbStr = Thread.currentThread().getContextClassLoader().getResource("krb5.conf").getFile();
			String hdfsuserkeytab = Thread.currentThread().getContextClassLoader().getResource("hdfs.headless.keytab")
					.getFile();

			System.setProperty("java.security.krb5.conf", krbStr);

			// 使用票据和凭证进行认证(需替换为自己申请的kerberos票据信息)
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(hdfsuserInfo, hdfsuserkeytab);
		}

		logger.info("FileSystem初始化....");

		FileSystem fs = HdfsUtils.createFileSystem(conf);
		HdfsUtils.printlist(fs, "/");
		return fs;
	}

}
