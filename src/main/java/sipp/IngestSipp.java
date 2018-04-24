package sipp;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
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
	
	private Configuration conf = null;
	
	public static void main(String[] args) throws IOException {
		IngestSipp sipp = new IngestSipp(args);
		sipp.bootstrap();		
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
		}
		
		return instance;
	}
	
	public Connector getConnector() {
		
		try {

			KerberosToken kt = new KerberosToken(clientConfig.getPrincipal(), new File(clientConfig.getKeytab()), false);
						
			Connector connector = getInstance().getConnector(clientConfig.getPrincipal(), kt);
			//DelegationToken dt = connector.securityOperations().getDelegationToken(new DelegationTokenConfig());
			return connector;
		} catch (AccumuloException | AccumuloSecurityException | IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void bootstrap() throws IOException {
		if(!validate())
			System.exit(1);
		
		conf = new Configuration();
//		System.err.println(IngestSipp.class.getResourceAsStream("/etc/hadoop/conf/core-site.xml") != null);
//		System.err.println(IngestSipp.class.getResourceAsStream("/etc/accumulo/conf/accumulo-site.xml") != null);
//		System.err.println(IngestSipp.class.getResourceAsStream("./core-site.xml") != null);
//		System.err.println(IngestSipp.class.getResourceAsStream("./accumulo-site.xml") != null);
//		System.err.println(IngestSipp.class.getResourceAsStream("core-site.xml") != null);
//		System.err.println(IngestSipp.class.getResourceAsStream("accumulo-site.xml") != null);
//		System.err.println(IngestSipp.class.getClassLoader().getResourceAsStream("/etc/hadoop/conf/core-site.xml") != null);
//		System.err.println(IngestSipp.class.getClassLoader().getResourceAsStream("/etc/accumulo/conf/accumulo-site.xml") != null);
//		System.err.println(IngestSipp.class.getClassLoader().getResourceAsStream("./core-site.xml") != null);
//		System.err.println(IngestSipp.class.getClassLoader().getResourceAsStream("./accumulo-site.xml") != null);
//		System.err.println(IngestSipp.class.getClassLoader().getResourceAsStream("core-site.xml") != null);
//		System.err.println(IngestSipp.class.getClassLoader().getResourceAsStream("accumulo-site.xml") != null);
		conf.addResource(IngestSipp.class.getClassLoader().getResourceAsStream("core-site.xml"));
		conf.addResource(IngestSipp.class.getClassLoader().getResourceAsStream("accumulo-site.xml"));
		
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

	private boolean validate() {
		Path attrFile = FileSystems.getDefault().getPath(clientConfig.getProperty("attributes.filename"));
		Path sippFile = FileSystems.getDefault().getPath(clientConfig.getProperty("sipp.data.filename"));
		
		if(!Files.exists(attrFile, LinkOption.NOFOLLOW_LINKS)) {
			System.out.println("Missing attributes file: " + attrFile.toString());
			return false;
		}
		
		if(!Files.exists(sippFile, LinkOption.NOFOLLOW_LINKS)) {
			System.out.println("Missing sipp data csv file: " + sippFile.toString());
			return false;
		}
		
		return true;
	}


	private void readSippData() {
		try {
			Scanner scanner = getConnector().createScanner(clientConfig.getTableName(), new Authorizations());
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
		
		BatchWriter writer = getBatchWriter(clientConfig.getTableName());
		
		BufferedReader reader = new BufferedReader(
				new FileReader(clientConfig.getProperty("sipp.data.filename")));
		
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
			return getConnector().createBatchWriter(clientConfig.getTableName(), bwOpts.getBatchWriterConfig());
		} catch (TableNotFoundException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private void loadAttributesFromFile() throws IOException {
		BufferedReader reader = new BufferedReader(
				new FileReader(clientConfig.getProperty("attributes.filename")));
				
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
		
		if(!ops.exists(clientConfig.getTableName())) {
			try {
				ops.create(clientConfig.getTableName());
			} catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
				// fail fast
				throw new RuntimeException(e);
			}
		}
		
	}

	private class ClientConfig extends Properties {

		private static final long serialVersionUID = 1L;

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
