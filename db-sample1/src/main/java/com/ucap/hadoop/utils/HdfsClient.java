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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


/**
 * 本地hadoop dfs文件系统客户端
 * @author mark
 */
public class HdfsClient {

	public static String DEFAULT_HDFS_URL = "hdfs://localhost:9000";

	private String hdfsUrl;

	private static HdfsClient instance;

	private HdfsClient(Configuration config) {
		String configFsUrl = config.get("fs.defaultFS");
		if (configFsUrl.length() > 10) {
			hdfsUrl = configFsUrl;
		} else {
			hdfsUrl = HdfsClient.DEFAULT_HDFS_URL;
		}
	}

	public static HdfsClient getInstance() {
		if (instance == null) {
			instance = new HdfsClient(new Configuration());
		}
		return instance;
	}

	/**
	 * @Title: 创建文件
	 * @Description: 创建文件
	 * @param file
	 *            文件
	 * @param 文件内容
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void createFile(String file, String content) throws IOException, InterruptedException {

		FileSystem fs = FileSystem.get(URI.create(hdfsUrl), new Configuration(), "root");
		byte[] buff = content.getBytes();
		FSDataOutputStream os = null;
		try {
			os = fs.create(new Path(file));
			os.write(buff, 0, buff.length);
			System.out.println("Create: " + file);
		} finally {
			if (os != null)
				os.close();
		}
		fs.close();
	}

	/**
	 *  
	 * 
	 * @Title: 创建路径
	 * @Description: 创建路径
	 * @param filePath
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 *                         Boolean
	 * @author mark
	 * @date 2018年4月10日下午5:09:25
	 */
	public Boolean mkdirs(String filePath) throws IOException, InterruptedException {
		boolean result = false;
		FileSystem fs = FileSystem.get(URI.create(hdfsUrl), new Configuration(), "root");
		Path path = new Path(filePath);
		result = fs.mkdirs(path);
		System.out.println(result);
		fs.close();
		return result;
	}

	/**
	 *  
	 * 
	 * @Title: 删除路径
	 * @Description: 删除路径
	 * @param filePath
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 *                         Boolean
	 * @author mark
	 * @date 2018年4月10日下午5:09:56
	 */
	public Boolean delete(String filePath) throws IOException, InterruptedException {
		boolean result = false;
		FileSystem fs = FileSystem.get(URI.create(hdfsUrl), new Configuration(), "root");
		Path deletePath = new Path(filePath);
		result = fs.delete(deletePath, true);
		System.out.println(result);
		fs.close();
		return result;
	}

	/**
	 *  
	 * 
	 * @Title: 重命名
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
	public Boolean rename(String oldPathString, String newPathString) throws IOException, InterruptedException {
		boolean result = false;
		FileSystem fs = FileSystem.get(URI.create(hdfsUrl), new Configuration(), "root");
		Path oldPath = new Path(oldPathString);
		Path newPath = new Path(newPathString);
		result = fs.rename(oldPath, newPath);
		fs.close();
		return result;
	}

	/**
	 *  
	 * 
	 * @Title: 将文件从HDFS拷贝到本地
	 * @Description: 将文件从HDFS拷贝到本地
	 * @param HDFSPathString
	 * @param LocalPathString
	 * @throws IOException
	 * @throws InterruptedException
	 *                         void
	 * @author mark
	 * @date 2018年4月10日下午5:10:45
	 */
	public void copyFileFromHDFS(String HDFSPathString, String LocalPathString)
			throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(URI.create(hdfsUrl), new Configuration(), "root");
		InputStream in = fs.open(new Path(HDFSPathString));
		OutputStream out = new FileOutputStream(LocalPathString);
		IOUtils.copyBytes(in, out, 4096, true);
		System.out.println("拷贝完成...");
		fs.close();
	}

	/**
	 *  
	 * 
	 * @Title: 从本地拷贝文件到HDFS
	 * @Description: 从本地拷贝文件到HDFS
	 * @param srcFile
	 * @param destPath
	 * @throws Exception
	 *                         void
	 * @author mark
	 * @date 2018年4月10日下午5:11:35
	 */
	public  void copyFileToHDFS(String srcFile, String destPath) throws Exception {
		FileInputStream fis = new FileInputStream(new File(srcFile));// 读取本地文件
		FileSystem fs = FileSystem.get(URI.create(hdfsUrl), new Configuration(), "root");
		OutputStream os = fs.create(new Path(destPath));
		// copy
		IOUtils.copyBytes(fis, os, 4096, true);
		System.out.println("拷贝完成...");
		fs.close();
	}

	/**
	 * /**  
	 * 
	 * @Title: 遍历文件夹和文件
	 * @Description: 遍历文件夹和文件
	 * @param folder
	 * @return List<String>
	 * @author mark
	 * @date 2018年4月10日下午5:12:08
	 */
	@SuppressWarnings("deprecation")
	public List<String> ls(String folder) {
		FileSystem fs = null;
		List<String> files = null;
		try {
			Path path = new Path(folder);
			files = new ArrayList<String>();
			fs = FileSystem.get(URI.create(hdfsUrl), new Configuration(), "root");
			FileStatus[] list = fs.listStatus(path);
			System.out.println("ls: " + folder);
			System.out.println("==========================================================");
			for (FileStatus f : list) {
				System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
				files.add(f.getPath().toUri().getPath());
			}
			System.out.println("==========================================================");
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return files;
	}

	/**
	 *  
	 * 
	 * @Title: 遍历文件夹和文件
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
	public List<String> recursiveHdfsPath(FileSystem hdfs, Path listPath)
			throws FileNotFoundException, IOException {
		List<String> list = new ArrayList<String>();
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
		//HdfsClient.getInstance().ls("/user/dblues");
		HdfsClient.getInstance().delete("/user/dblues/wordcount/apioutput");
		
	}

}
