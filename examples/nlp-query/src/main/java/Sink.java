import java.io.FileNotFoundException;
import java.io.PrintWriter;

import edu.stanford.nlp.ling.CoreAnnotations.LBeginAnnotation;
import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;


public class Sink implements SeepTask {
	
	PrintWriter out;
	@Override
	public void setUp() {
    System.out.println("[SINK]");
//		try {
//			out = new PrintWriter ("times.txt");
//		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

	@Override
	public void processData(ITuple data, API api) {
    String query = data.getString("query");
		long ts = data.getLong("ts");
		long kb_time = data.getLong("kb_time");
		long current = System.currentTimeMillis();
		
//	System.out.println("QUERY: " + query + " TIME: " + (current - ts - kb_time) + " KB: " + kb_time);
//		System.out.println("[Sink] QUERY: " + query);// + " TIME: " + (current - ts - kb_time) + " KB: " + kb_time);
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		out.close();
	}

}
