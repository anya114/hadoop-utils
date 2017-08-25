package com.inspur.tax.connectpool;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Connect {

   
    private String createTime;

    private static Logger logger =LoggerFactory.getLogger(Connect.class);   
    
    SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
    
    /**
     * 初始化Conn对象，模拟创建Conn对象平均消耗500ms
     * @throws InterruptedException
     */
    public Connect() throws InterruptedException {
        Thread.sleep(500);
        
        createTime = sdFormatter.format(new Date());
        
        logger.info( createTime+"  初始化对象成功......... " );
    }

    /**
     * 报告Conn对象信息
     */
    public void report() {
    
        createTime = sdFormatter.format(new Date());
        logger.info(createTime+" 可用对象... " );
    }
}