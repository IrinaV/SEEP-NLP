import java.math.BigInteger;
import java.util.Properties;
import java.util.Random;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.LogicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.QueryBuilder;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seepcontrib.kafka.config.KafkaConfig;

public class Base implements QueryComposer {
	
  @Override
	public LogicalSeepQuery compose() {

		Schema schema = SchemaBuilder.getInstance()
				.newField(Type.STRING, "query")
				.newField(Type.LONG, "ts")
				.newField(Type.LONG, "kb_time")
				.build();
				
		LogicalOperator src = queryAPI.newStatelessSource(new Source(), 0);

    LogicalOperator nlp = queryAPI.newStatelessOperator(new NLP(), 1);
    LogicalOperator kb = queryAPI.newStatelessOperator(new KB(), 2);

		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), 3);

   src.connectTo(nlp, 0, schema);
   nlp.connectTo(kb, 0, schema);
   kb.connectTo(snk, 0, schema);    

   queryAPI.setInitialPhysicalInstancesForLogicalOperator(1, 3);
   queryAPI.setInitialPhysicalInstancesForLogicalOperator(2, 6);
		return QueryBuilder.build();
	}
}
