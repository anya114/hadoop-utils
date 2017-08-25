package com.inspur.tax.connectpool;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.fs.FileSystem;

/**
 * common-pool2 使用方式
 * <p/>
 * Conn对象管理池，这里利用GenericObjectPool作为对象池
 *
 */

public class ConnPool extends GenericObjectPool<FileSystem> {


    /**
     * 调用{@link GenericObjectPool}的构造方法，构造ConnPool
     */
    public ConnPool() {
        super(new ConnFactory(), new ConnPoolConfig());
    }

    /**
     * 调用{@link GenericObjectPool}的构造方法，构造ConnPool
     */
    public ConnPool(ConnPoolConfig connPoolConfig) {
        super(new ConnFactory(), connPoolConfig);
    }

}
