package net.ldbc.snb.janusgraph.importer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class LoadEdgesThread extends Thread {
		
	private JanusGraph graph;
	private String[] colNames;
	private String idLabelV1;
	private String idLabelV2;
	private long txMaxRetries;
	private String[] threadLines;
	private long lineCount;
	private String edgeLabel;
	private boolean undirected;
	
	public LoadEdgesThread(
			JanusGraph graph,
			String[] colNames,
			String idLabelV1,
			String idLabelV2,
			String edgeLabel,
			boolean undirected,
			long txMaxRetries,
			String[] threadLines,
			long lineCount)
	{
		this.graph = graph;
		this.colNames = colNames;
		this.idLabelV1 = idLabelV1;
		this.idLabelV2 = idLabelV2;
		this.txMaxRetries = txMaxRetries;
		this.threadLines = threadLines;
		this.lineCount = lineCount;
		this.edgeLabel = edgeLabel;
		this.undirected = undirected;
	}
	
	@Override
	public void run() {

		SimpleDateFormat creationDateDateFormat = 
				new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		SimpleDateFormat joinDateDateFormat = 
				new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		joinDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		boolean txSucceeded = false;
		int txFailCount = 0;
		do {
			JanusGraphTransaction tx = graph.newTransaction();
			for (int i = 0; i < threadLines.length; i++) {
				String line = threadLines[i];

				String[] colVals = line.split("\\|");

				GraphTraversalSource g = tx.traversal();
				Vertex vertex1 = 
						g.V().has(idLabelV1, Long.parseLong(colVals[0])).next();
				Vertex vertex2 = 
						g.V().has(idLabelV2, Long.parseLong(colVals[1])).next();

				HashMap<Object, Object> propertiesMap = new HashMap<>();
				for (int j = 2; j < colVals.length; ++j) {
					if (colNames[j].equals("creationDate")) {
						try {
							propertiesMap.put(colNames[j], 
									creationDateDateFormat.parse(colVals[j]).getTime());
						} catch (ParseException e) {
							e.printStackTrace();
						}
					} else if (colNames[j].equals("joinDate")) {
						try {
							propertiesMap.put(colNames[j], 
									joinDateDateFormat.parse(colVals[j]).getTime());
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}else if(colNames[j].equals("workFrom")) {
						propertiesMap.put(colNames[j], Integer.parseInt(colVals[j]));
					}else if(colNames[j].equals("classYear")) {
						propertiesMap.put(colNames[j], Integer.parseInt(colVals[j]));
					}else {
						propertiesMap.put(colNames[j], colVals[j]);
					}
				}

				List<Object> keyValues = new ArrayList<Object>();
				propertiesMap.forEach((key, val) -> {
					keyValues.add(key);
					keyValues.add(val);
				});

				vertex1.addEdge(edgeLabel, vertex2, keyValues.toArray());

				if (undirected) {
					vertex2.addEdge(edgeLabel, vertex1, keyValues.toArray());
				}

			}

			try {
				tx.commit();
				txSucceeded = true;
			} catch (Exception e) {
				txFailCount++;
			}

			if (txFailCount > txMaxRetries) {
				throw new RuntimeException(String.format(
						"ERROR: Transaction failed %d times, (file lines [%d,%d])" +  
								"aborting...", txFailCount, lineCount + 2, (lineCount + 2) + (threadLines.length - 1)));
			}
		} while (!txSucceeded);
	}
}
