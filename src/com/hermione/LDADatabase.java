package com.hermione;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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

public class LDADatabase {
	private Connection con = null;
	private int taskId = -1;
	private String fileDir;
	HashMap<Integer, String[]> contentsMap = new HashMap();
	ArrayList<String> topicNum;
	public HashMap<Integer, String[]> getContentsMap() {
		return contentsMap;
	}
	public LDADatabase(String fileDir, int taskId) throws IOException{
		this.fileDir = fileDir;
		this.taskId = taskId;
		this.parse();
		this.parseTopicNum();
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
	// parse files that under fileDir
	public void parse() throws IOException{
		String keyWordPath = this.fileDir + "/topic.kwordsR" + this.taskId;
		String phrasePath = this.fileDir + "/topic.phraseR" + this.taskId;
		String contentPath = this.fileDir + "/topic.origin" + this.taskId;
		
		HashMap<Integer, String> keyWordMap = new HashMap();
		HashMap<Integer, String> phraseMap = new HashMap();
		HashMap<Integer, String> originMap = new HashMap();

		// Map<Integer, String[]>
		int topicNum = -1;
		try {
			FileInputStream kfs= new FileInputStream(keyWordPath);
			BufferedReader kbr = new BufferedReader(new InputStreamReader(kfs, "UTF-8"));
			String line = null;
			StringBuilder tmp = new StringBuilder();
			while((line = kbr.readLine()) != null){
					String[] parts = line.trim().split("\\s+");
					if("topic".equals(parts[0])){
						if(!"".equals(tmp.toString())){
//							System.out.println(topicNum+":"+tmp.toString());
							keyWordMap.put(topicNum, tmp.toString());
						}
						topicNum = Integer.parseInt(parts[1]);
						tmp =  new StringBuilder();
					}
					else{
						tmp.append(parts[0]);
						tmp.append(",");
					}
			}
			//last 
			keyWordMap.put(topicNum, tmp.toString());
//			System.out.println(topicNum+":"+tmp.toString());

			FileInputStream pfs= new FileInputStream(phrasePath);
			BufferedReader pbr = new BufferedReader(new InputStreamReader(pfs, "UTF-8"));
			tmp = new StringBuilder();
			while((line = pbr.readLine()) != null){
					String[] parts = line.trim().split("\\s+");
					if("topic".equals(parts[0])){
						if(!"".equals(tmp.toString())){
//							System.out.println(topicNum+":"+tmp.toString());
							phraseMap.put(topicNum, tmp.toString());
						}
						topicNum = Integer.parseInt(parts[1]);
						tmp =  new StringBuilder();
					}
					else{
						parts = line.trim().split("\t");
						tmp.append(parts[0]);
						tmp.append(",");
					}
					
			}
			//last
//			System.out.println(topicNum+":"+tmp.toString());
			phraseMap.put(topicNum, tmp.toString());

			FileInputStream cfs= new FileInputStream(contentPath);
			BufferedReader cfr = new BufferedReader(new InputStreamReader(cfs, "UTF-8"));
			tmp = new StringBuilder();
			while((line = cfr.readLine()) != null){
				String[] parts = line.trim().split("\\s+");
				if("topic".equals(parts[0])){
					if(!"".equals(tmp.toString())){
//						System.out.println(topicNum+":"+tmp.toString());
						originMap.put(topicNum, tmp.toString());
					}
					topicNum = Integer.parseInt(parts[1]);
					tmp =  new StringBuilder();
				}
				else{
					tmp.append(line.trim());
					tmp.append("|^|");
				}
			}
			
//			System.out.println(topicNum+":"+tmp.toString());
			originMap.put(topicNum, tmp.toString());
			
			// put all contents into contentsMap
			topicNum = keyWordMap.size();
			
			for(int topic_i=0; topic_i<topicNum;topic_i++){
				String[] contents = new String[3];
				if (keyWordMap.containsKey(topic_i)){
					contents[0] = keyWordMap.get(topic_i);
				}else{
					contents[0] = "null";
				}
				if (phraseMap.containsKey(topic_i)){
					contents[1] = phraseMap.get(topic_i);
				}else{
					contents[1] = "null";
				}
				if (originMap.containsKey(topic_i)){
					contents[2] = originMap.get(topic_i);
				}else{
					contents[2] = "null";
				}
				contentsMap.put(topic_i, contents);
			}
			try {
				kfs.close();
				kbr.close();
				pfs.close();
				pbr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("can not open file");
		}
		
	}
	
	public void parseTopicNum() throws IOException{
	    topicNum = new ArrayList();
		String originNumPath = this.fileDir + "/topic.num" + this.taskId;
		FileInputStream nfs= new FileInputStream(originNumPath);
		BufferedReader nfr = new BufferedReader(new InputStreamReader(nfs, "UTF-8"));
		String line = null;
		while((line = nfr.readLine()) != null){
			topicNum.add(line.trim());
		}
	}
	
	public void insert(String[] contents, int topicId){
		String keyWords = contents[0];
		String phrases = contents[1];
		String content = contents[2];
		String sql = "insert into topic_results(task_id, keywords, phrases, content, created_at, updated_at, num) values (?,?,?,?,?,?,?)";
		try {
			PreparedStatement pst = con.prepareStatement(sql);
			Date created_at = new Date();
			Date updated_at = new Date();
			Timestamp ct = new Timestamp(created_at.getTime());
			Timestamp ut = new Timestamp(updated_at.getTime());
			con.setAutoCommit(false);
			pst.setInt(1, taskId);
			pst.setString(2, keyWords);
			pst.setString(3, phrases);
			pst.setString(4, content);
			pst.setTimestamp(5, ct);
			pst.setTimestamp(6, ut);
			pst.setString(7, topicNum.get(topicId));
			pst.executeUpdate();
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	
	public static void main(String[] args) throws IOException{
		int taskId = Integer.parseInt(args[0]);
		System.out.println("taskId: "+taskId + " saving topic result");
		String fileDir = args[1];
		
		LDADatabase ldaDatabase = new LDADatabase(fileDir, taskId);
		ldaDatabase.connection();
		HashMap<Integer, String[]> contentsMap = ldaDatabase.getContentsMap();
//		System.out.println(contentsMap.size());
		Set<Integer> keySet = contentsMap.keySet();
		for(Integer topicId : keySet){
			ldaDatabase.insert(contentsMap.get(topicId), topicId);
//			System.out.println(contentsMap.get(topicId)[2]);
		}
		ldaDatabase.close();
		
		// for keyWords
		
		KeyWordDataBase keyWordDataBase = new KeyWordDataBase(fileDir,taskId);
		System.out.println("taskId: "+taskId + " saving keyWords result");
		System.out.println("finished");
		
	}
}






















