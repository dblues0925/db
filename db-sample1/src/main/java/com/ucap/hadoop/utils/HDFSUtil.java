package com.ucap.hadoop.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUtil {
	public static String HDFS_URL = "hdfs://192.168.186.132:9000";

	/**
	 *  
	 * 
	 * @Title: addPath
	 * @Description: 创建路径
	 * @param filePath
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 *                         Boolean
	 * @author mark
	 * @date 2018年4月10日下午5:09:25
	 */
	public static Boolean addPath(String filePath) throws IOException, InterruptedException {
		boolean result = false;
		FileSystem fs = FileSystem.get(URI.create(HDFSUtil.HDFS_URL), new Configuration(), "root");
		Path path = new Path(filePath);
		result = fs.mkdirs(path);
		System.out.println(result);
		return result;
	}

	/**
	 *  
	 * 
	 * @Title: deletePath
	 * @Description: 删除路径
	 * @param filePath
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 *                         Boolean
	 * @author mark
	 * @date 2018年4月10日下午5:09:56
	 */
	public static Boolean deletePath(String filePath) throws IOException, InterruptedException {
		boolean result = false;
		FileSystem fs = FileSystem.get(URI.create(HDFSUtil.HDFS_URL), new Configuration(), "root");
		Path deletePath = new Path(filePath);
		result = fs.delete(deletePath, true);
		System.out.println(result);
		return result;
	}

	/**
	 *  
	 * 
	 * @Title: ReName
	 * @Description: 重命名
	 * @param oldPathString
	 * @param newPathString
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 *                         Boolean
	 * @author mark
	 * @date 2018年4月10日下午5:10:12
	 */
	public static Boolean ReName(String oldPathString, String newPathString) throws IOException, InterruptedException {
		boolean result = false;
		FileSystem fs = FileSystem.get(URI.create(HDFSUtil.HDFS_URL), new Configuration(), "root");
		Path oldPath = new Path(oldPathString);
		Path newPath = new Path(newPathString);
		result = fs.rename(oldPath, newPath);
		return result;
	}

	/**
	 *  
	 * 
	 * @Title: copyFileFromHDFS
	 * @Description: 将文件从HDFS拷贝到本地
	 * @param HDFSPathString
	 * @param LocalPathString
	 * @throws IOException
	 * @throws InterruptedException
	 *                         void
	 * @author mark
	 * @date 2018年4月10日下午5:10:45
	 */
	public static void copyFileFromHDFS(String HDFSPathString, String LocalPathString)
			throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(URI.create(HDFSUtil.HDFS_URL), new Configuration(), "root");
		InputStream in = fs.open(new Path(HDFSPathString));
		OutputStream out = new FileOutputStream(LocalPathString);
		IOUtils.copyBytes(in, out, 4096, true);
		System.out.println("拷贝完成...");
	}

	/**
	 *  
	 * 
	 * @Title: copyFileToHDFS
	 * @Description: 从本地拷贝文件到HDFS
	 * @param srcFile
	 * @param destPath
	 * @throws Exception
	 *                         void
	 * @author mark
	 * @date 2018年4月10日下午5:11:35
	 */
	public static void copyFileToHDFS(String srcFile, String destPath) throws Exception {

		FileInputStream fis = new FileInputStream(new File(srcFile));// 读取本地文件
		FileSystem fs = FileSystem.get(URI.create(HDFSUtil.HDFS_URL), new Configuration(), "root");
		OutputStream os = fs.create(new Path(destPath));
		// copy
		IOUtils.copyBytes(fis, os, 4096, true);
		System.out.println("拷贝完成...");
		fs.close();
	}

	/**
	 *  
	 * 
	 * @Title: recursiveHdfsPath
	 * @Description: 遍历文件夹和文件
	 * @param hdfs
	 * @param listPath
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 *                         List<String>
	 * @author mark
	 * @date 2018年4月10日下午5:12:08
	 */
	public static List<String> recursiveHdfsPath(FileSystem hdfs, Path listPath)
			throws FileNotFoundException, IOException {
		List<String> list = new ArrayList();
		FileStatus[] files = null;
		files = hdfs.listStatus(listPath);
		for (FileStatus f : files) {
			if (files.length == 0 || f.isFile()) {
				list.add(f.getPath().toUri().getPath());
			} else {
				list.add(f.getPath().toUri().getPath());
				// 是文件夹，且非空，就继续遍历
				recursiveHdfsPath(hdfs, f.getPath());
			}
		}
		for (String a : list) {
			System.out.println(a);
		}
		return list;
	}

	public static void main(String[] args) throws Exception {
		FileSystem hdfs = FileSystem.get(URI.create(HDFSUtil.HDFS_URL), new Configuration(), "root");
		HDFSUtil.recursiveHdfsPath(hdfs, new Path("/"));
		//HDFSUtil.copyFileFromHDFS("/r11i.txt", "E://r11i.txt");
		//HDFSUtil.copyFileToHDFS("E:/my1.png", "/my1.png");
	}

}
