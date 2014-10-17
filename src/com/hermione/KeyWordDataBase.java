package com.hermione;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;

public class KeyWordDataBase {
	private Connection con = null;
	private int taskId = -1;
	private String fileDir;
	String keyWords;
	public KeyWordDataBase(String fileDir,int taskId ) throws IOException{
		this.taskId = taskId;
		this.fileDir = fileDir;
		this.connection();
		this.parse();
		this.insert();
		this.close();
	}
	public void connection(){
		String url = "jdbc:mysql://117.79.239.109:3306/Movie";
//		String url = "jdbc:mysql://210.14.138.8:3306/Movie";
		String userName = "root";
		String passWord = "root";
		try {
			 Class.forName("com.mysql.jdbc.Driver");
			 this.con = DriverManager.getConnection(url, userName, passWord);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("database connection failed");
		}
	}
	public void close(){
		if(this.con !=null){
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public void parse() throws IOException{
		this.keyWords = null;
		StringBuilder  keyWordsTmp = new StringBuilder();
		String keyWordPath = this.fileDir + "/keyWords" + this.taskId;
		FileInputStream kfs = new FileInputStream(keyWordPath);
		BufferedReader kbr = new BufferedReader(new InputStreamReader(kfs,"UTF-8"));
		String line = null;
		while((line = kbr.readLine())!=null){
			String word = line.trim();
			keyWordsTmp.append(word+",");
		}
		keyWords = keyWordsTmp.toString();
	}
	
	public void insert(){
		String sql = "insert into keyword_results(task_id, keywords, created_at, updated_at) values (?,?,?,?)";
		try {
			PreparedStatement pst = con.prepareStatement(sql);
			Date created_at = new Date();
			Date updated_at = new Date();
			Timestamp ct = new Timestamp(created_at.getTime());
			Timestamp ut = new Timestamp(updated_at.getTime());
			con.setAutoCommit(false);
			
			pst.setInt(1, taskId);
			pst.setString(2, keyWords);
			pst.setTimestamp(3, ct);
			pst.setTimestamp(4, ut);
			pst.executeUpdate();
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
