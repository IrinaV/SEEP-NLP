import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import yago2.normalize.Normalize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class KB implements SeepTask {

  private Schema schema = SchemaBuilder.getInstance()
      .newField(Type.STRING, "query").newField(Type.LONG, "ts")
      .newField(Type.LONG, "kb_time").build();
  
  private List<String> hosts;
  private int index = 0;

  @Override
  public void setUp() {
    System.out.println("[KB]");
    hosts = new ArrayList<String>();
    for (int i = 1; i <= 16; ++i)
      hosts.add("http://wombat16.doc.res.ic.ac.uk:" + String.valueOf(8000 + i) + "/query");
//    for (int i = 1; i <= 16; ++i)
//      hosts.add("http://wombat17.doc.res.ic.ac.uk:" + String.valueOf(8000 + i) + "/query");
    Collections.shuffle(hosts);
  }
  
  @Override
  public void processData(ITuple data, API api) {
    String query = data.getString("query");
    Long ts = data.getLong("ts");
    String[] sub_pred = query.split("@");
    String sub = sub_pred[0];
    String pred = sub_pred[1];

    try {
    	long kb_start = System.currentTimeMillis();
        URL url = new URL(hosts.get(index));
        index = (index + 1 == hosts.size() ? 0 : index + 1);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);

        OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
        
        JSONObject obj = new JSONObject();
        obj.put("subject", sub);
        obj.put("predicate", pred);
        writer.write(obj.toJSONString());
        writer.flush();
        writer.close();
        
        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        Object outObj = JSONValue.parse(reader.readLine());
        JSONArray res = (JSONArray) outObj;
        reader.close();
       
        long kb_end = System.currentTimeMillis();
        long kb_time = kb_end - kb_start;
    //    if (res.size() > 0) {
          //System.out.println("[res] " + res.get(0));
          byte[] processedData = OTuple.create(schema, new String[] { "query", "ts", "kb_time" }, new Object[] { "", ts, kb_time});//res.get(0), ts, kb_time } );
          api.send(processedData);
  //    }
    } catch (IOException e) {
      e.printStackTrace();
    }    
  }
  
  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public void processDataGroup(ITuple arg0, API arg1) {
    // TODO Auto-generated method stub

  }

}

