package com.inspur.tax.common.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JAVA 操作HDFS公共工具类
 * 
 * @author Jerry.R
 * @param
 * @date 2017年2月18日下午3:48:56
 */
public class HdfsUtils {

	private static final Logger logger = LoggerFactory.getLogger(HdfsUtils.class);

	/**
	 * 创建FileSystem
	 * 
	 * @param conf
	 * @return FileSystem
	 * @throws Exception
	 */
	public static FileSystem createFileSystem(Configuration conf) throws IOException {

		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("createFileSystemException", e);
			throw new IOException("创建FileSystem错误", e);
		}

		return fs;

	}

	/**
	 * 关闭FileSystem
	 * 
	 * @param fs
	 * @return boolean
	 * @throws Exception 
	 */
	public static boolean closeFileSystem(FileSystem fs) throws IOException {
		// 判断
		try {
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("closeFileSystemException", e);
			throw new IOException("关闭FileSystem错误",e);
		}

		return true;

	}

	/**
	 * HDFS中新建目录
	 * 
	 * @param dir
	 * @param fs
	 * @throws IOException
	 */
	public static boolean mkdir(String dir, FileSystem fs) throws IOException {
		if (StringUtils.isBlank(dir)) {
			return false;
		}
		boolean result = true;
		if (!fs.exists(new Path(dir))) {
			try {
				fs.mkdirs(new Path(dir));
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("MkdirException", e);
				throw new IOException("MkdirException", e);
			}
		}

		return result;
	}

	/**
	 * HDFS 删除HDFS下的目录
	 * 
	 * @param dir
	 * @param fs
	 * @throws IOException
	 */
	public static boolean deleteDir(String dir, FileSystem fs) throws IOException {
		if (StringUtils.isBlank(dir)) {
			return false;
		}
		boolean result = true;
		try {
			fs.delete(new Path(dir), true);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			logger.error("deleteDirException", e);
			throw new IOException("deleteDirException", e);
		}

		return result;
	}

	/**
	 * 列出参数目录下HDFS
	 * 
	 * @param fs
	 * @param pathstr
	 */

	public static void printlist(FileSystem fs, String pathstr) throws IOException {
		FileStatus[] list;
		try {
			list = fs.listStatus(new Path(pathstr));
			for (FileStatus file : list) {
				logger.info(file.getPath().getName());
			}
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("PrintListException", e);
			throw new IOException("PrintListException", e);
		}
	}

	/**
	 * 本地文件方式上传到 HDFS
	 * 
	 * @param localFile
	 *            本地文件的绝对路径
	 * @param hdfsFile
	 *            HDFS上的路径
	 * @return boolean status
	 */
	public static boolean uploadLocalFileToHDFS(String localFile, String hdfsFile, FileSystem hdfs) throws IOException {
		if (StringUtils.isBlank(localFile) || StringUtils.isBlank(hdfsFile)) {
			return false;
		}
		boolean result = true;
		try {
			Path src = new Path(localFile);
			Path dsf = new Path(hdfsFile);
			hdfs.copyFromLocalFile(src, dsf);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("copyFromLocalFileException", e);
			throw new IOException("copyFromLocalFileException", e);
		}

		return result;
	}

	/**
	 * 文件流方式上传到 HDFS
	 * 
	 * @param localFileInputStream
	 *            上传的文件流
	 * @param hdfsFilepath
	 *            上传的HDFS路径
	 * @param hdfs
	 *            filesystem 类
	 */
	public static boolean uploadFileStreamToHDFS(InputStream localFileInputStream, String hdfsFilepath, FileSystem hdfs)
			throws IOException {
		boolean result = true;

		try {
			FSDataOutputStream hdfsFileOutputStream = hdfs.create(new Path(hdfsFilepath), true);

			IOUtils.copyBytes(localFileInputStream, hdfsFileOutputStream, 1024, true);
		} catch (IllegalArgumentException e1) {
			e1.printStackTrace();
			logger.error("uploadFileStreamToHDFSException", e1);
			throw new IOException("uploadFileStreamToHDFSException", e1);

		}

		return result;
	}

	/**
	 * HDFS 目录下新建文件
	 * 
	 * @param newFile
	 * @param content
	 * @return
	 * @throws IOException
	 */
	public static boolean createNewHDFSFile(String newFile, String content, FileSystem fs) throws IOException {

		if (StringUtils.isBlank(newFile) || null == content) {
			return false;
		}
		boolean result = true;
		FSDataOutputStream os = null;
		try {
			os = fs.create(new Path(newFile));
			os.write(content.getBytes("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("createNewHDFSFileException", e);
			throw new IOException("createNewHDFSFileException", e);
		} finally {
			if (os != null) {
				os.close();
			}

		}

		return result;
	}

	/**
	 * HDFS 删除目录下文件
	 * @param hdfsFile
	 *            FileSystem
	 * @return boolean
	 * @throws IOException
	 */
	public static boolean deleteHDFSFile(String hdfsFile, FileSystem fs) throws IOException {

		if (StringUtils.isBlank(hdfsFile)) {
			return false;
		}

		boolean isDeleted = true;
		try {
			Path path = new Path(hdfsFile);
			isDeleted = fs.delete(path, true);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("DeleteException", e);
			throw new IOException("DeleteException", e);
		}

		return isDeleted;
	}

	/**
	 * HDFS 读取HDFS文件内容
	 * @param hdfsFile
	 * @return boolean
	 * @throws Exception
	 */
	public static byte[] readHDFSFile(String hdfsFile, FileSystem fs) throws Exception {

		if (StringUtils.isBlank(hdfsFile)) {
			throw new Exception("HDFS路径为空");
		}

		Path path = new Path(hdfsFile);
		FSDataInputStream is = null;
		byte[] buffer = null;

		try {
			is = fs.open(path);
			FileStatus stat = fs.getFileStatus(path);
			buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
			is.readFully(0L, buffer);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("readFileException", e);
			throw new IOException("readFileException", e);
		} finally {
			if (is != null) {
				is.close();
			}
		}

		return buffer;
	}

	/**
	 * HDFS 在HDFS文件中追加内容
	 * 
	 * @param hdfsFile
	 *            需要追加内容的HDFS文件
	 * @parm 需要追加的具体内容
	 * @return boolean
	 * @throws Exception
	 */
	public static boolean append(String hdfsFile, String content, FileSystem fs, Configuration conf) throws Exception {
		if (StringUtils.isBlank(hdfsFile)) {
			return false;
		}
		if (StringUtils.isEmpty(content)) {
			return true;
		}

		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");

		Path path = new Path(hdfsFile);
		Boolean result = true;

		if (fs.exists(path)) {
			InputStream in = null;
			OutputStream out = null;
			try {
				in = new ByteArrayInputStream(content.getBytes());
				out = fs.append(new Path(hdfsFile));

				IOUtils.copyBytes(in, out, 4096, true);

			} catch (Exception e) {

				logger.error("appendException", e);
				e.printStackTrace();
				throw new Exception("追加发生错误", e);

			} finally {
				if (in != null) {
					in.close();
				}
				if (out != null) {
					out.close();
				}
			}
		} else {
			try {
				HdfsUtils.createNewHDFSFile(hdfsFile, content, fs);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("createNewHDFSFileException", e);
				throw new Exception("createNewHDFSFileException", e);

			}
		}

		return result;
	}

	/**
	 * 移动HDFS上的文件位置
	 * 
	 * @param hdfsFileBefore
	 * @param hdfsFileAfter
	 * @param fs
	 * @throws Exception
	 */

	public static boolean moveFileOnHDFS(String hdfsFileBefore, String hdfsFileAfter, FileSystem fs) throws Exception {
		boolean result = true;
		try {
			Path pathBefore = new Path(hdfsFileBefore);
			Path pathAfter = new Path(hdfsFileAfter);
			fs.rename(pathBefore, pathAfter);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("moveFileOnHDFSException", e);
			throw new Exception("moveFileOnHDFSException", e);
		}

		return result;

	}

}