package sipp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

public class IngestSipp {
	
	private static final SimpleDateFormat frm = new SimpleDateFormat("yyyy-MM-dd");
	private BatchWriter batchWriter;
	private InputStream fileInputStream;
	private final ColumnVisibility vis = new ColumnVisibility();
	
	private static final String SIPP_TABLENAME = "sipp";
	
	private List<String> attributes = new ArrayList<>();
	private static Instance instance;
	
	
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
				
//		ClientOnRequiredTable opts = new ClientOnRequiredTable();
//		
//		opts.parseArgs("Accumulo Load Symbol", args, new Object[]{});
//		
//		BatchWriterOpts bopts = new BatchWriterOpts();
//		IngestSipp load = new IngestSipp();
//		Connector con = opts.getConnector();
//		
//		load.batchWriter = con.createBatchWriter(opts.getTableName(), bopts.getBatchWriterConfig());
//		
//		for (SymbolLine sl : load.getAllSymbolLinesFromFile()) 
//			load.batchWriter.addMutation(load.createMutation(sl.getSymbol(), sl.getDate(), sl.getAttibute(), sl.getValue()));
//		
//		load.batchWriter.flush();
		
		IngestSipp sipp = new IngestSipp();
		sipp.bootstrap();
		//sipp.loadSippData();
		
		
	}
	
	public IngestSipp() {
		
	}
	
	public Instance getInstance() {
		if(instance == null) {
//			MiniAccumuloCluster accumulo;
//			try {
//				accumulo = new MiniAccumuloCluster(new File("Z:\\gleec001\\accumulo"), "password");
//				accumulo.start();
//			} catch (IOException | InterruptedException e) {
//				throw new RuntimeException(e);
//			}
			
//			instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
			instance = new MockInstance("testinstance");
		}
		
		return instance;
	}
	
	public Connector getConnector() {
		
		
		try {
			return getInstance().getConnector("root", new PasswordToken("password"));
		} catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
		
		
	}
	
	private void bootstrap() throws IOException {
		createSippTable();
		
		try {
			loadSippData();
			
			// Print out the table information
			readSippData();
		} catch (MutationsRejectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	private void readSippData() {
		try {
			Scanner scanner = getConnector().createScanner(SIPP_TABLENAME, new Authorizations());
			for(Entry<Key, Value> entry: scanner) {
				Key k = entry.getKey();
				Value v = entry.getValue();
				
				System.out.println(k.getRow().toString() + "\t" + k.getColumnFamily().toString() + "\t" +
						k.getColumnQualifier().toString() + "\t" + k.getColumnVisibility().toString() + 
						k.getTimestamp() + " => " + v.toString());
				
			}
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void loadSippData() throws IOException, MutationsRejectedException {
		// Fetch data
		loadAttributesFromFile();
		
		BatchWriter writer = getBatchWriter(SIPP_TABLENAME);
		
		BufferedReader reader = new BufferedReader(
				new FileReader(IngestSipp.class.getClassLoader().getResource("sipp_attributes.txt").getPath()));
		
		String line;
				
		int rowCounter = 1;
		
		while((line = reader.readLine()) != null) {
			String rowId = "row" + rowCounter++;
			Mutation mutation = new Mutation(rowId);
			String[] fields = line.split(",");
			for(int i = 0; i < fields.length; i++) {
				mutation.put("attributes", attributes.get(i), fields[i]);
				writer.addMutation(mutation);
			}
		}
		
		writer.flush();
		writer.close();
		reader.close();	
		
	}
	
	private BatchWriter getBatchWriter(String tablename) {
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		try {
			return getConnector().createBatchWriter(SIPP_TABLENAME, bwOpts.getBatchWriterConfig());
		} catch (TableNotFoundException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private void loadAttributesFromFile() throws IOException {
		BufferedReader reader = new BufferedReader(
				new FileReader(IngestSipp.class.getClassLoader().getResource("sipp_attributes.txt").getPath()));
				
		String line;
		while((line = reader.readLine()) != null) {
			attributes.add(line);
		}
		
		reader.close();
	}

	/**
	 * This will create a new Sipp table if it doesn't already
	 * exist
	 */
	private void createSippTable() {
		TableOperations ops = getConnector().tableOperations();
		
		if(ops.exists(SIPP_TABLENAME)) {
			try {
				ops.create(SIPP_TABLENAME);
			} catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
				// fail fast
				throw new RuntimeException(e);
			}
		}
		
	}

}
