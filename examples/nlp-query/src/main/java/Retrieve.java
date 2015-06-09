import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import yago2.normalize.*;

public class Retrieve {

	public static void main(String[] args) {
		
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		
		String url = "jdbc:postgresql://localhost/mydb";
		String user = "irina";
		String password = "123";
		
		try {
			
			con = DriverManager.getConnection(url, user, password);
			pst = con.prepareStatement("select object from yagofacts where subject='<Albert_Einstein>' and predicate='<wasBornIn>';");
			rs = pst.executeQuery();
			
			while (rs.next()) {
				System.out.println(Normalize.unNormalize(rs.getString("object")));
			}
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(Retrieve.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		} finally {
			
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
		}
	}
	
//	private Connection con = null;
//
//	public Retrieve(String user, String password) {
//		DBAccess access = new DBAccess(user, password);
//		con = access.getConnection();
//	}

//	public List<String> getFacts(String arg1, String relation) {
//		if (arg1 != null)
//			arg1 = Normalize.entity(arg1);
//		List<String> facts = null;
//		try {
//			Statement stmt = con.createStatement();
//			ResultSet rs = null;
//			if (relation == null || relation.isEmpty())
//				return null;
//			rs = stmt.executeQuery("select * from facts where (arg1='"+arg1+"' and relation='"+relation+"')");
//			facts = new ArrayList<String>();
//			while (rs.next())
//				if (!facts.contains(Normalize.unEntity(rs.getString("arg2"))))
//					facts.add(Normalize.unEntity(rs.getString("arg2")));
//			rs.close();
//			stmt.close();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		if (facts == null || facts.isEmpty())
//			return null;
//		return facts;
//	}
//	
//	public List<String> getPreferredMeanings(String arg1) {
//		return getFacts(arg1, "hasPreferredMeaning");
//	}
}
