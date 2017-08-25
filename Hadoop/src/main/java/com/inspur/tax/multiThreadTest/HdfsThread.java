package com.inspur.tax.multiThreadTest;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.inspur.tax.common.hdfs.HdfsUtils;
import com.inspur.tax.hadoop.HadoopKerberos;

public class HdfsThread extends Thread {
	public HdfsThread(String name) {
		super(name);
	}

	public void run() {
		FileSystem fs = null;
		try {
			fs = HadoopKerberos.init();
//			HadoopKerberos.uploadFile(fs);

		} catch (IOException e) {
			e.printStackTrace();

		} finally {
			HdfsUtils.closeFileSystem(fs);

		}

	}
}
