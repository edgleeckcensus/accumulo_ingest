package sipp;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

public class IngestSipp {
	
	private static final SimpleDateFormat frm = new SimpleDateFormat("yyyy-MM-dd");

	private List<String> attributes = new ArrayList<>();
	private static Instance instance;

	private static ClientConfig clientConfig = null;
	
	public static void main(String[] args) throws IOException {
				
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
		
		IngestSipp sipp = new IngestSipp(args);

		sipp.bootstrap();
		//sipp.loadSippData();
		
		
	}


	public IngestSipp(String[] args) {
		loadConfiguration(args);
	}

	private void loadConfiguration(String[] args) {
		if (args.length == 0) {
			System.err.println("Properties file required");
			System.exit(1);
		}

		clientConfig = new ClientConfig(args);

	}

	public Instance getInstance() {
		if(instance == null) {
			instance = new ZooKeeperInstance(clientConfig.getInstanceName(), clientConfig.getZookeepers());
//			instance = new MockInstance();
		}
		
		return instance;
	}
	
	public Connector getConnector() {
		
		
		try {

			Connector connector = getInstance().getConnector(clientConfig.getPrincipal(),
					new KerberosToken(clientConfig.getPrincipal(), new File(clientConfig.getKeytab()), true));
			return connector;
		} catch (AccumuloException | AccumuloSecurityException | IOException e) {
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
				new FileReader(IngestSipp.class.getClassLoader().getResource(SIPP_DATA).getPath()));
		
		String line;
				
		int rowCounter = 1;
		
		while((line = reader.readLine()) != null) {
			String rowId = "row" + rowCounter++;
			Mutation mutation = new Mutation(rowId);
			String[] fields = line.split(",");
			for(int i = 0; i < fields.length; i++) {
				mutation.put("attributes", attributes.get(i), fields[i]);
			}
			writer.addMutation(mutation);
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
		
		if(!ops.exists(SIPP_TABLENAME)) {
			try {
				ops.create(SIPP_TABLENAME);
			} catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
				// fail fast
				throw new RuntimeException(e);
			}
		}
		
	}

	private class ClientConfig extends Properties {

		public ClientConfig(String[] args) {
			try {
				this.load(new FileReader(args[0]));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public String getTableName() {
			return getProperty("sipp.table.name");
		}

		public String getPrincipal() {
			return getProperty("principal");
		}

		public String getKeytab() {
			return getProperty("keytab");
		}

		public String getZookeepers() {
			return getProperty("zookeepers");
		}

		public String getInstanceName() {
			return getProperty("instance.name");
		}


	}


}
