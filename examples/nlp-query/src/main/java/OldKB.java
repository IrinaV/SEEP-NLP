import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import yago2.normalize.Normalize;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class OldKB implements SeepTask {

  private Schema schema = SchemaBuilder.getInstance()
      .newField(Type.STRING, "query").newField(Type.LONG, "ts")
      .newField(Type.LONG, "kb_time").build();

   private String path =
       "/home/iv511/SEEPng/examples/nlp-query/src/main/java/sql_questions_gin";
   String[] content = new String[2];

  static List<String> res = new ArrayList<String>();
  static Connection con = null;
  static PrintWriter writer;

  @Override
  public void setUp() {
    System.out.println("[PROCESSOR]");
    long ts_setup = System.currentTimeMillis();
    try {
      connectToDatabase();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    try {
      writer = new PrintWriter("KB_output.txt", "UTF-8");
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // Added for the case to evaluate only the KB performance
     try {
      content = OpenFile();
     } catch (IOException e) {
      e.printStackTrace();
     }

    long te_setup = System.currentTimeMillis();
    System.out.println("[SETUP TIME] " + (te_setup - ts_setup));
  }

  @Override
  public void processData(ITuple data, API api) {
    System.out.println("[PROCESSOR]");
    long ts = System.currentTimeMillis();
    // long ts_process = System.currentTimeMillis();
    String text = data.getString("query");
    long kb_time = data.getLong("kb_time");
    String word = "";
    res.clear();
    // create an empty annotation just with the given text
    
     
    // For KB ONLY
    for (String query : content) {
      long start = System.currentTimeMillis();
      queryKB(query);
      long end = System.currentTimeMillis();
      kb_time = end - start;
      if (res.size() > 0) {
        byte[] processedData = OTuple.create(schema, new String[] {
            "query", "ts", "kb_time" }, new Object[] { res.get(0),
            ts, kb_time });
        api.send(processedData);
      }
      res.clear();
    }
  }

  // }
  /*
   * /** Calculates the similarity (a number within 0 and 1) between two
   * strings.
   */
  public static double similarity(String s1, String s2) {
    String longer = s1, shorter = s2;
    if (s1.length() < s2.length()) {
      longer = s2;
      shorter = s1;
    }
    int longerLength = longer.length();
    if (longerLength == 0) {
      return 1.0; /* both strings are zero length */
    }
    return (longerLength - editDistance(longer, shorter))
        / (double) longerLength;
  }

  public static int editDistance(String s1, String s2) {
    s1 = s1.toLowerCase();
    s2 = s2.toLowerCase();

    int[] costs = new int[s2.length() + 1];
    for (int i = 0; i <= s1.length(); i++) {
      int lastValue = i;
      for (int j = 0; j <= s2.length(); j++) {
        if (i == 0)
          costs[j] = j;
        else {
          if (j > 0) {
            int newValue = costs[j - 1];
            if (s1.charAt(i - 1) != s2.charAt(j - 1))
              newValue = Math.min(Math.min(newValue, lastValue),
                  costs[j]) + 1;
            costs[j - 1] = lastValue;
            lastValue = newValue;
          }
        }
      }
      if (i > 0)
        costs[s2.length()] = lastValue;
    }
    return costs[s2.length()];
  }

  private static List<String> matchPred(String word,
      HashMap<String, String> pred_set) {
    double max_score = 0.0;
    List<String> best_match = new ArrayList<String>();
    for (Object iter : pred_set.entrySet()) {
      HashMap.Entry pair = (HashMap.Entry) iter;
      String key = (String) pair.getKey();
      String value = (String) pair.getValue();
      double sim = similarity(word, value);
      if (sim > max_score) {
        max_score = sim;
        best_match.clear();
      }
      if (sim == max_score) {
        best_match.add(key);
      }
    }
    return best_match;
  }

  private void connectToDatabase() throws ClassNotFoundException {

    String url = "jdbc:postgresql://localhost/mydb";
    String user = "irinaveliche";
    String password = "123";

    try {
      OldKB.class.forName("org.postgresql.Driver");
      con = DriverManager.getConnection(url, user, password);
    } catch (SQLException ex) {
      Logger lgr = Logger.getLogger(Retrieve.class.getName());
      lgr.log(Level.SEVERE, ex.getMessage(), ex);
    }
  }

  private static void queryKB(String query) {
    PreparedStatement pst = null;
    ResultSet rs = null;

    try {
      pst = con.prepareStatement(query);

      rs = pst.executeQuery();
      while (rs.next()) {
        res.add(Normalize.unNormalize(rs.getString("object")));
        writer.println(Normalize.unNormalize(rs.getString("object")));
        writer.flush();
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
      } catch (SQLException ex) {
        Logger lgr = Logger.getLogger(Retrieve.class.getName());
        lgr.log(Level.WARNING, ex.getMessage(), ex);
      }
    }
  }

  private String[] OpenFile() throws IOException {
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

  @Override
  public void processDataGroup(ITuple dataBatch, API api) {
    // TODO Auto-generated method stub
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }
}
