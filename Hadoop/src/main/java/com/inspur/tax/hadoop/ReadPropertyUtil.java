package com.inspur.tax.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadPropertyUtil {
	public static final Logger logger = LoggerFactory.getLogger(ReadPropertyUtil.class);

	public static Properties readProperty() {

		InputStream inputStream = ReadPropertyUtil.class.getClassLoader().getResourceAsStream("hdfs.properties");
		Properties p = new Properties();
		try {
			p.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("ReadPropertyException:", e);
		}
		logger.info("nameservices:" + p.getProperty("nameservices"));
		return p;
	}

	public static Configuration getConfiguration() {

		Properties p = ReadPropertyUtil.readProperty();

		Configuration conf = new Configuration();
		String nameservices = p.getProperty("nameservices");
		logger.info("nameservice: "+nameservices);
		conf.set("fs.defaultFS", "hdfs://" + nameservices);
		// 被指定为ACL的集群管理员 相关参数具体值在idap中查看
		conf.set("dfs.cluster.administrators", p.getProperty("dfs.cluster.administrators"));
		conf.set("dfs.nameservices", nameservices);
		conf.set("dfs.ha.namenodes." + nameservices, "nn1,nn2");
		conf.set("dfs.namenode.rpc-address." + nameservices + ".nn1", p.getProperty("dfs.namenode.rpc-address1"));
		conf.set("dfs.namenode.rpc-address." + nameservices + ".nn2", p.getProperty("dfs.namenode.rpc-address2"));
		conf.set("dfs.client.failover.proxy.provider."+nameservices, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		//启用Kerberos后加载
		boolean isUseKerberos=Boolean.valueOf(ReadPropertyUtil.readProperty().getProperty("isUseKerberos"));
		if (isUseKerberos) {
			conf.set("dfs.namenode.kerberos.principal.pattern",
					p.getProperty("dfs.namenode.kerberos.principal.pattern"));
			conf.set("hadoop.security.authorization", p.getProperty("hadoop.security.authorization"));
			conf.set("hadoop.security.authentication", p.getProperty("hadoop.security.authentication"));
		}
		return conf;

	}

}
