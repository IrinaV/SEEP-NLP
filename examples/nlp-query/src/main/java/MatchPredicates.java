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
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import com.codahale.metrics.*;

import java.util.concurrent.TimeUnit;

public class MatchPredicates {

	private static StanfordCoreNLP pipeline;
	static Properties props = new Properties();
	static {
		props.setProperty("annotators",
				"tokenize, ssplit, pos, lemma, ner");
		pipeline = new StanfordCoreNLP(props);
	}

	private static String path = "/Users/irinaveliche/ImperialCollege/master/SEEPng/examples/simple-pipeline-stateless-query/src/main/java/questions";
	static String[] content = new String[2];

	static HashMap<String, String> pred_location = new HashMap<String, String>();
	static HashMap<String, String> pred_time = new HashMap<String, String>();
	static HashMap<String, String> pred_entity = new HashMap<String, String>();
	static HashMap<String, String> pred_other = new HashMap<String, String>();
	static HashMap<String, String> subjects = new HashMap<String, String>();

	public static final String[] word_location = new String[] { "where" };
	public static final Set<String> q_location = new HashSet<String>(
			Arrays.asList(word_location));
	public static final String[] word_time = new String[] { "when" };
	public static final Set<String> q_time = new HashSet<String>(
			Arrays.asList(word_time));

	static final MetricRegistry metrics = new MetricRegistry();
	private static Meter requests = metrics.meter("requests");
	static Connection con = null;

	public static void printMap(HashMap mp) {
		for (Object iter : mp.entrySet()) {
			HashMap.Entry pair = (HashMap.Entry) iter;
			System.out.println(pair.getKey() + ": " + pair.getValue());
		}
	}

	public static void main(String[] args) {
		ExtractPredicates.getPredicates(pred_location, pred_time, pred_entity,
				pred_other);
//		try {
//			content = OpenFile();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		String url = "jdbc:postgresql://localhost/mydb";
		String user = "irinaveliche";
		String password = "123";

		try {
			con = DriverManager.getConnection(url, user, password);
//			pst = con.prepareStatement("create index subject_idx on yagofacts using gin (subject gin_trgm_ops);");
//			rs = pst.executeQuery();
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(Retrieve.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		}

		startReport();
//		for (int times = 0; times < 1000; ++times) {
//			for (String query : content) {
		String query = "What prize did Albert Einstien win?";
		match(query);
//			}
//			++times;
//		}
	}

	private static void match(String query) {
		String text = query;
		String word = "";
		// create an empty annotation just with the given text
		Annotation document = new Annotation(text);

		// run all Annotators on this text
		pipeline.annotate(document);

		// these are all the sentences in this document
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);

		for (CoreMap sentence : sentences) {
			HashMap<String, String> predicates = new HashMap<String, String>();
			String subject = "";
			String match_pred = "";

			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				word = token.get(TextAnnotation.class);
				// this is the POS tag of the token
				String pos = token.get(PartOfSpeechAnnotation.class);
				String lemma = token.get(LemmaAnnotation.class);
				String ner = token.get(NamedEntityTagAnnotation.class);

				System.out.println("WORD: " + word + " POS: " + pos +
						 " LEMMA: " + lemma + " NER: " + ner);
				if (pos.equals("WRB")) {
					if (q_location.contains(lemma)) {
						predicates.putAll(pred_location);
					} else if (q_time.contains(lemma)) {
						predicates.putAll(pred_time);
					}
					continue;
				} else {
					predicates.putAll(pred_entity);
					predicates.putAll(pred_other);
				}
				if (!ner.equals("O")) {
					subject += lemma + "_";
				} else if (pos.equals(".") || pos.equals("WP")
						|| pos.equals("WDT")) { // ignore
					// "What"
					// and stop
					// marks
					continue;
				} else {
					match_pred += lemma + " ";
				}

			}
			// printMap(predicates);
			// System.out.println("[SUBJECT] " + subject);
			// System.out.println("PREDICATE TO BE MATCHED: " + match_pred);
			List<String> matched_pred = matchPred(match_pred, predicates);
			// List<String> matched_subjects = matchPred(subject, subjects);
			long start = System.currentTimeMillis();
			for (int i = 0; i < matched_pred.size(); ++i) {
				// for (int j = 0; j < matched_subjects.size(); ++j) {
				dbConnection(subject.substring(0, subject.length() - 1),
						matched_pred.get(i));
				requests.mark();
			}
			long end = System.currentTimeMillis();
			System.out.println("KB TIME: " + (end - start));
		}
	}

	static void startReport() {
		ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS).build();
		reporter.start(15, TimeUnit.SECONDS);
	}

	static void wait5Seconds() {
		try {
			Thread.sleep(5 * 1000);
		} catch (InterruptedException e) {
		}
	}

	/**
	 * Calculates the similarity (a number within 0 and 1) between two strings.
	 */
	public static double similarity(String s1, String s2) {
		String longer = s1, shorter = s2;
		if (s1.length() < s2.length()) { // longer should always have greater
											// length
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

	// Example implementation of the Levenshtein Edit Distance
	// See http://rosettacode.org/wiki/Levenshtein_distance#Java
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

	public static void printSimilarity(String s, String t) {
		System.out.println(String.format(
				"%.3f is the similarity between \"%s\" and \"%s\"",
				similarity(s, t), s, t));
	}

	private static List<String> matchPred(String word,
			HashMap<String, String> pred_set) {
		System.out.println("[predicate to be matched]" + word);
		double max_score = 0.0;
		List<String> best_match = new ArrayList<String>();
		for (Object iter : pred_set.entrySet()) {
			HashMap.Entry pair = (HashMap.Entry) iter;
			String key = (String) pair.getKey();
			String value = (String) pair.getValue();
//			System.out.println("[predSet]" + value);
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

	private static void dbConnection(String subject, String predicate) {
//		System.out.println("[subject] " + subject);
//		System.out.println("[predicate] " + predicate);
//		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		System.out.println("[SUBJECT]:" + subject);
		System.out.println("[PREDICATE]:" + predicate);

//		String url = "jdbc:postgresql://localhost/mydb";
//		String user = "irinaveliche";
//		String password = "123";

		try {

//			con = DriverManager.getConnection(url, user, password);
			/*String sql = "select object from (select distinct object, subject <-> '"
					+ subject
					+ "' as dist from yagofacts where predicate='"
					+ predicate
					+ "') as foo where dist = (select min(dist) from (select distinct object, subject <-> '"
					+ subject
					+ "' as dist from yagofacts where predicate='"
					+ predicate + "') as bar);";
*/
			//String sql = "select distinct object from yagofacts where predicate='" + predicate + "' and subject like '%" + subject + "%';";

			String sql = ("select distinct object from yagofacts where to_tsvector('simple', regexp_replace(subject, E'[^A-Za-z0-9]', ' ', 'g'))"
					+ " @@ to_tsquery('" + subject + "') and predicate='<" + predicate + ">';");
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

	private static String[] OpenFile() throws IOException {
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

	private static int readLines() throws IOException {
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

}