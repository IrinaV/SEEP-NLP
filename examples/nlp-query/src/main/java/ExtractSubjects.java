import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class ExtractSubjects {

	private String path = "subjectA.txt";

	ExtractSubjects(String file_path) {
		path = file_path;
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

	static HashMap<String, String> subjects = new HashMap<String, String>();

	private static String removeBrackets(String pred) {
		if (pred.length() >= 3)
			return pred.substring(2, pred.length() - 1);
		else
			return pred;
	}

	private static String tokenize(String pred) {
		String[] tokens = pred.split("(?=[A-Z])");
		String res = "";
		for (int i = 0; i < tokens.length; ++i) {
			res += tokens[i];
		}
		return res;
	}

	public static void getSubjects(HashMap<String, String> subjects) {

		ExtractSubjects extractPredicates = new ExtractSubjects(
				"/Users/irinaveliche/ImperialCollege/master/SEEPng/examples/simple-pipeline-stateless-query/src/main/java/subjectsA.txt");
		String[] content = new String[2];
		try {
			content = extractPredicates.OpenFile();
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (String subject : content) {
			String subject_key = removeBrackets(subject);
			//String words_from_pred = tokenize(subject_key);

			subjects.put(subject, subject_key);
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
		getSubjects(subjects);
		printMap(subjects);
	}
}
