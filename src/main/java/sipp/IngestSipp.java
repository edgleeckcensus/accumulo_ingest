package sipp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
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
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.conf.Configuration;

import conf.ClientConfig;

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
			return connector;
		} catch (AccumuloException | AccumuloSecurityException | IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void bootstrap() throws IOException {
		if(!validate())
			System.exit(1);
		
		createSippTable();
		
		try {
			loadSippData();
			
			// Print out the table information
			//readSippData();
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
				mutation.put("attributes", attributes.get(i), fields[i].trim());
			}
			writer.addMutation(mutation);
			if(rowCounter % 100 == 0) System.out.println("...added " + rowCounter );
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
			attributes.add(line.trim());
		}
		
		reader.close();
	}

	/**
	 * This will create a new Sipp table if it doesn't already
	 * exist
	 */
	private void createSippTable() {
		TableOperations tableOps = getConnector().tableOperations();
		SecurityOperations secOps = getConnector().securityOperations();
		
		
		try {
			if (!tableOps.exists(clientConfig.getTableName())) {
				tableOps.create(clientConfig.getTableName());
			}
			
			// set the correct permissions
			secOps.grantTablePermission(clientConfig.getPrincipal(), clientConfig.getTableName(), TablePermission.READ);
			secOps.grantTablePermission(clientConfig.user(), clientConfig.getTableName(), TablePermission.WRITE);
			secOps.grantSystemPermission(clientConfig.getPrincipal(), SystemPermission.OBTAIN_DELEGATION_TOKEN);
			

		} catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
			// fail fast
			throw new RuntimeException(e);
		}	
		
		
	}

	

}
