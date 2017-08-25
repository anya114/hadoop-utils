package com.inspur.tax.hadoop.Hadoop;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;

import com.inspur.tax.hadoop.HadoopKerberos;

public class HadoopKerberosTest {

	@Before
	public  void setUp() throws Exception {
		
		HadoopKerberos.init();
	}

	@Test
	public void testInit() {
		
	}

	@Test
	public void testUploadFile() throws IOException  {
		
		
		
	}

}
