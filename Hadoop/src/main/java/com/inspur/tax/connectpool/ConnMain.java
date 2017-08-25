package com.inspur.tax.connectpool;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnMain {
    
	
	public static Logger logger= LoggerFactory.getLogger(ConnMain.class);
    public static void main(String[] args) throws Exception {
        ConnPoolConfig ConnPoolConfig = new ConnPoolConfig();
        ConnPoolConfig.setMinIdle(5);
        ConnPoolConfig.setMaxIdle(8);
        ConnPool ConnPool = new ConnPool(ConnPoolConfig);
        FileSystem FileSystem1 = ConnPool.borrowObject();
        FileSystem FileSystem2 = ConnPool.borrowObject();
        FileSystem FileSystem3 = ConnPool.borrowObject();
        FileSystem FileSystem4 = ConnPool.borrowObject();
        FileSystem FileSystem5 = ConnPool.borrowObject();
       
        
        //ConnPool.returnObject(FileSystem1);
        logger.info("NumIdle:  " +String.valueOf(ConnPool.getNumIdle()));
        logger.info("NumActive:  "+ String.valueOf(ConnPool.getNumActive()));
        
        ConnPool.returnObject(FileSystem2);
        
       
        logger.info("NumIdle:  " +String.valueOf(ConnPool.getNumIdle()));  //return 
        logger.info("NumActive:  "+ String.valueOf(ConnPool.getNumActive()));  //Active
        ConnPool.returnObject(FileSystem3);
        
        ConnPool.returnObject(FileSystem4);
        
        ConnPool.returnObject(FileSystem5);


        // 被归还的对象的引用，不可以再次归还
        // java.lang.IllegalStateException: Object has already been retured to this pool or is invalid
        try {
            ConnPool.returnObject(FileSystem2);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}