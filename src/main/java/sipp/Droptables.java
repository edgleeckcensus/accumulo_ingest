package sipp;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;

import conf.ClientConfig;

public class Droptables {
	

	private static Instance instance;

	private static ClientConfig clientConfig = null;
	
	
	public static void main(String[] args) throws IOException {
		Droptables sipp = new Droptables(args);
		sipp.bootstrap();		
	}


	public Droptables(String[] args) {
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
		
		dropSippTable();
		
		
		
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

	

	/**
	 * This will create a new Sipp table if it doesn't already
	 * exist
	 */
	private void dropSippTable() {
		TableOperations tableOps = getConnector().tableOperations();
		
		for(String table : tableOps.list()) {
			if (table.contains("row")) {
				try {
					tableOps.delete(table);
					System.out.println("Dropped " + table);
				} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
		
		
	}

	

}
