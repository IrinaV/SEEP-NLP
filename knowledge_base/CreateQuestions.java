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

public class CreateQuestions {
	
	static Connection con = null;

	public static void main(String[] args) {
	
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
		dbConnection();
	}

	private static void dbConnection() {

		PreparedStatement pst = null;
		ResultSet rs = null;
		
		try {
	
			String sql = ("select distinct subject from yagofacts where predicate='<hasOfficialLanguage>' LIMIT 100;");
			
			pst = con.prepareStatement(sql);
			rs = pst.executeQuery();

			while (rs.next()) {
				System.out
						.println(Normalize.unNormalize(rs.getString("subject")));
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
}