import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class ExtractPredicates {

	private String path = "predicates.txt";

	ExtractPredicates(String file_path) {
		path = file_path;
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

	public static final String[] prep_entity = new String[] { "with", "to",
			"for", "of", "in" };
	public static final Set<String> entity = new HashSet<String>(
			Arrays.asList(prep_entity));
	public static final String[] prep_time = new String[] { "until", "on",
			"since" };
	public static final Set<String> time = new HashSet<String>(
			Arrays.asList(prep_time));
	public static final String[] prep_location = new String[] { "from", "at",
			"in" };
	public static final Set<String> location = new HashSet<String>(
			Arrays.asList(prep_location));

	private static StanfordCoreNLP pipeline;
	static Properties props = new Properties();
	static {
		props.setProperty("annotators",
				"tokenize, ssplit, pos, lemma, parse, ner");
		pipeline = new StanfordCoreNLP(props);
	}

	static HashMap<String, String> pred_location = new HashMap<String, String>();
	static HashMap<String, String> pred_time = new HashMap<String, String>();
	static HashMap<String, String> pred_entity = new HashMap<String, String>();
	static HashMap<String, String> pred_other = new HashMap<String, String>();

	public static List<List<String>> lemmatize(String predicate) {
		List<String> lemmas = new ArrayList<String>();
		String lemmaString = "";
		List<String> lemmaList = new ArrayList<String>();
		List<String> attribute = new ArrayList<String>();
		boolean third_type_pred = false;
		// Create an empty Annotation just with the given text
		Annotation document = new Annotation(predicate);
		// run all Annotators on this text
		pipeline.annotate(document);
		// Iterate over all of the sentences found
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			// Iterate over all tokens in a sentence
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				// Retrieve and add the lemma for each word into the
				// list of lemmas
				String pos = token.get(PartOfSpeechAnnotation.class);
				if (pos.equals("TO")) {
					attribute.add("entity");
				} else if (pos.equals("IN")) {
					if (entity.contains(token.lemma()))
						attribute.add("entity");
					if (time.contains(token.lemma()))
						attribute.add("time");
					else if (location.contains(token.lemma()))
						attribute.add("location");
				}
//				if (token.originalText().equals("has")) {
//					third_type_pred = true;
//				}
//				if (third_type_pred) {
//					lemmaString += token.originalText() + " ";
//				} else {
					String lemma = token.get(LemmaAnnotation.class);
					lemmaList.add(lemma);
					//lemmaString += lemma + " ";
					// lemmas.add(lemma);
//				}
			}
		}
		Collections.sort(lemmaList);
		for (int i = 0; i < lemmaList.size(); ++i) {
			lemmaString += lemmaList.get(i) + " ";
		}
		lemmas.add(lemmaString);
		List<List<String>> res = new ArrayList<>();
		res.add(lemmas);
		res.add(attribute);
		return res;
	}

	private static String removeBrackets(String pred) {
		return pred.substring(1, pred.length() - 1);
	}

	private static String[] tokenize(String pred) {
		String[] res = pred.split("(?=[A-Z])");
		return res;
	}

	private static String normalize(String[] words) {
		String res = "";
		for (int i = 0; i < words.length; ++i)
			res += (words[i].toLowerCase()) + " ";
		return res;
	}

	public static void getPredicates(HashMap<String, String> pred_location,
			HashMap<String, String> pred_time,
			HashMap<String, String> pred_entity,
			HashMap<String, String> pred_other) {

		ExtractPredicates extractPredicates = new ExtractPredicates(
				"/Users/irinaveliche/ImperialCollege/master/SEEPng/examples/simple-pipeline-stateless-query/src/main/java/pred_no_xml"); //predicates");
		String[] content = new String[2];
		try {
			content = extractPredicates.OpenFile();
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (String pred : content) {
			String pred_key = pred;//removeBrackets(pred);
			String[] words_from_pred = tokenize(pred_key);
			String normalized_words = normalize(words_from_pred);
			List<List<String>> lemmas = lemmatize(normalized_words);

			List<String> keywordsList = lemmas.get(0);
			Collections.sort(keywordsList);
			String keywords = keywordsList.get(0);
//			System.out.println("[KEYWORDS]" + keywords);
			List<String> attributes = lemmas.get(1);
			if (attributes.size() == 0) {
				pred_other.put(pred, keywords);
			} else {
				for (int i = 0; i < attributes.size(); ++i) {
					if (attributes.get(i).equals("location")) {
						pred_location.put(pred, keywords);
					} else if (attributes.get(i).equals("time")) {
						pred_time.put(pred, keywords);
					} else if (attributes.get(i).equals("entity")) {
						pred_entity.put(pred, keywords);
					}
				}
			}
		}
	}

	public static void printMap(HashMap mp) {
		Iterator it = mp.entrySet().iterator();
		while (it.hasNext()) {
			HashMap.Entry pair = (HashMap.Entry) it.next();
			System.out.println(pair.getKey() + ": " + pair.getValue());
			it.remove(); // avoids a ConcurrentModificationException
		}
	}

	public static void main(String[] args) {

		getPredicates(pred_location, pred_time, pred_entity, pred_other);
		System.out.println("LOCATION: ");
		printMap(pred_location);
		System.out.println("TIME: ");
		printMap(pred_time);
		System.out.println("ENTITY: ");
		printMap(pred_entity);
		System.out.println("OTHER: ");
		printMap(pred_other);
	}
}
