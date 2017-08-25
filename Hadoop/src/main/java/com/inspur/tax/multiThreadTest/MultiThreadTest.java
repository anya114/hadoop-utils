package com.inspur.tax.multiThreadTest;

public class MultiThreadTest {

	public static void main(String[] args) {

		for (int i = 0; i < 1000; i++) {
			
			String name = "thread" + i;
			
			HdfsThread thread = new HdfsThread(name);
			
			thread.start();

		}

	}

}
