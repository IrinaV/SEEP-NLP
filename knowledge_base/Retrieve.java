package experiment_KB;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import yago2.normalize.*;

public class Retrieve {

	private static int count = 0;
	private static int[] counters = new int[50];
	
	public static void main(String[] args) {
		
		List<String> subjects = Arrays.asList("Albert_Einstein", "Barack_Obama", "Ella_Fitzgerald", "Goldman_Sachs", "Samoa", "Irene_Ware",
				"Bryan_College", "John_Selden", "Helen_Miller", "Adam_Chandler", "Katie_Wright", "Nigeria", "Vietnam", "George_Shaw", "Gilberto_Gil", "Gord_Perks",
				"David_Hume", "Grenada");
		List<String> predicates = Arrays.asList("isMarriedTo", "isMarriedTo", "hasMusicalRole", "owns", "hasCapital", "actedIn", "hasMotto",
				"isInterestedIn", "isLeaderOf", "hasChild", "actedIn", "hasCapital", "hasCapital", "wroteMusicFor", "wroteMusicFor", "isLeaderOf", "isInterestedIn", "hasCapital");
		
		class Bench implements Runnable {

			private int index = 0;
			private int seconds = 0;
			private List<String> hosts;
			
			public Bench(int seconds, int numWorkers) {
				this.seconds = seconds;
				hosts = new ArrayList<String>();
				for (int i = 1; i <= numWorkers / 2; ++i)
					hosts.add("http://wombat16.doc.res.ic.ac.uk:" + String.valueOf(8000 + i) + "/query");
				for (int i = 1; i <= numWorkers / 2; ++i)
					hosts.add("http://wombat17.doc.res.ic.ac.uk:" + String.valueOf(8000 + i) + "/query");
				Collections.shuffle(hosts);
			}
			
			@Override
			public void run() {
				long timestamp = System.currentTimeMillis();
				while (System.currentTimeMillis() - timestamp < 1000 * seconds) {
					for (int i = 0; i < 18; ++i) {
					try {
						URL url = new URL(hosts.get(index));
						++counters[index];
						index = (index + 1 == hosts.size() ? 0 : index + 1);
						
						HttpURLConnection conn = (HttpURLConnection) url.openConnection();
						conn.setRequestMethod("POST");
						conn.setDoOutput(true);

						
					    OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
					    
					    JSONObject obj = new JSONObject();
//					    obj.put("subject", "Albert_Einstein");
//					    obj.put("predicate", "wasBornIn");
					    obj.put("subject", subjects.get(i));
					    obj.put("predicate", predicates.get(i));
					    writer.write(obj.toJSONString());
					    writer.flush();
					    writer.close();
					    
					    String line;
					    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
					    Object outObj = JSONValue.parse(reader.readLine());
					    JSONArray array= (JSONArray) outObj;
					    reader.close();
					    
					    ++count;
					    //System.out.println("First item: " + array.get(0));
					} catch (IOException e) {
						e.printStackTrace();
					}
					}
				}	
			}
		}
		
		int seconds = 5;
		int noThreads = 32;
		int noWorkers = 32;
		ArrayList<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < noThreads; ++i) {
			Bench bench = new Bench(seconds, noWorkers);
			threads.add(new Thread(bench));
		}
		for (int i = 0; i < noThreads; ++i) {
			threads.get(i).start();
		}
		for (int i = 0; i < noThreads; ++i) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("Send " + count + " requests.");
		System.out.println("Rate per thread: " + (count / seconds / noThreads) + " requests / second");
		System.out.println("Rate overall: " + (count / seconds) + " requests / second");
		
		System.out.println("Load distribution:");
		for (int i = 0; i < noWorkers; ++i) {
			System.out.print(counters[i] + " ");
		}
		System.out.println("\n");
	}
	
}
