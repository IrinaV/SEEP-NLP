package experiment_KB;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import yago2.normalize.Normalize;

import java.util.concurrent.TimeUnit;

public class GetAnswers {
	
	static Connection con = null;
	private String path = "predicates.txt";

	GetAnswers(String file_path) {
		path = file_path;
	}

	
	public static void main(String[] args) {
	
		GetAnswers extractPredicates = new GetAnswers(
				"/Users/irinaveliche/ImperialCollege/master/experiment_KB/src/subjects");
		String[] content = new String[2];
		try {
			content = extractPredicates.OpenFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		PreparedStatement pst = null;
		ResultSet rs = null;

		String url = "jdbc:postgresql://localhost/mydb";
		String user = "irinaveliche";
		String password = "123";

		try {
			con = DriverManager.getConnection(url, user, password);
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(Retrieve.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		}
		for (String sub : content) {
			dbConnection(sub);
			
		}
		
	}

	private static void dbConnection(String sub) {

		PreparedStatement pst = null;
		ResultSet rs = null;
		
		try {
	
			String sql = ("select distinct object from yagofacts where predicate='<hasOfficialLanguage>' and subject='" + sub + "';");
			
			pst = con.prepareStatement(sql);
			rs = pst.executeQuery();

			while (rs.next()) {
				System.out
						.println(Normalize.unNormalize(rs.getString("object")));
			}
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(Retrieve.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		} /*finally {

			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(Retrieve.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}*/
	}
	
	String[] OpenFile() throws IOException {
		FileReader fr = new FileReader(path);
		BufferedReader textReader = new BufferedReader(fr);
		int num_of_lines = readLines();
		String[] textData = new String[num_of_lines];

		for (int i = 0; i < num_of_lines; ++i) {
			textData[i] = textReader.readLine();
		}

		textReader.close();
		return textData;
	}

	private int readLines() throws IOException {
		FileReader file_to_read = new FileReader(path);
		BufferedReader bf = new BufferedReader(file_to_read);

		String aLine;
		int num_of_lines = 0;

		while ((aLine = bf.readLine()) != null) {
			++num_of_lines;
		}
		bf.close();
		return num_of_lines;
	}
	
	private static void waitHere(int time){
		try {
			Thread.sleep(time);
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}