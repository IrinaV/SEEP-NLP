import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class Source implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance()
			.newField(Type.STRING, "query").newField(Type.LONG, "ts")
			.newField(Type.LONG, "kb_time").build();

	private String path = "/home/iv511/SEEPng/examples/nlp-query/src/main/java/questions";
	String[] content = new String[2];
	private int times = 0;
	long ts = 0;//System.currentTimeMillis();
	long kb_time = 0;
	
	@Override
	public void setUp() {
    System.out.println("[SOURCE]");
		try {
			content = OpenFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
    System.out.println("[times]: " + times);
	}

	@Override
	public void processData(ITuple data, API api) {
//		if (times <= 2) {
			System.currentTimeMillis();
			for (String query : content) {
				//System.out.println("QUESTION: " + query);
				byte[] d = OTuple.create(schema, new String[] { "query", "ts", "kb_time" },
						new Object[] { query, ts, kb_time });
				api.send(d);

				//waitHere(10);
			}
//			++times;
//		}
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

	private void waitHere(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
