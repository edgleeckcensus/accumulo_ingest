package mapreduce;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.beust.jcommander.internal.Lists;

import sipp.IngestSipp;

public class IngestSippData extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(IngestSippData.class);
	private static final String PROPERTIES_FILE = "client-properties.xml";
	

	public static void main(String[] args) throws Exception  {
		Configuration conf = new Configuration();
		conf.addResource(IngestSipp.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE));
		System.err.println(IngestSipp.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE) != null);
		
		// add the wave, we'll use this later
		String[] pathFields = conf.get("input.path").split("/");
		String[] waveKeyValue = pathFields[pathFields.length - 1].split("=");
		conf.set("wave", waveKeyValue[1]);
		System.out.println("Wave = " + conf.get("wave"));
		
		int exitCode = ToolRunner.run(conf, new IngestSippData(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
				
		// Configure the parameters for the job
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("Ingest SIPP To Accumulo");
		job.addCacheFile(new URI(conf.get("attributes.filename")));
		job.setJarByClass(IngestSippData.class);
		job.setMapperClass(IngestSippMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);
		job.setNumReduceTasks(0);	
		
		// Configure the input format
		for(String path : conf.get("input.path").split(",")) 
			TextInputFormat.addInputPath(job, new Path(path));
			    
		
		// Configure the security settings for the job
	    KerberosToken kt = new KerberosToken(conf.get("principal"), new File(conf.get("keytab")), false);
	    ZooKeeperInstance instance = new ZooKeeperInstance(conf.get("instance.name"), conf.get("zookeepers"));
	    Connector connector = instance.getConnector(conf.get("principal"), kt);
	    DelegationToken dt = connector.securityOperations().getDelegationToken(null);
	    
	    
		// Configure the output format
		AccumuloOutputFormat.setConnectorInfo(job, conf.get("principal"), dt);
	    AccumuloOutputFormat.setCreateTables(job, true);
	    AccumuloOutputFormat.setDefaultTableName(job, conf.get("sipp.table.name"));
	    
	    AccumuloOutputFormat.setZooKeeperInstance(job, ClientConfiguration.loadDefault()
	    		.withInstance(conf.get("instance.name"))
	    		.withZkHosts(conf.get("zookeepers")));

	    
	    /* 
	     * Bootsrap the accumulo table, which creates
	     * the table if it doesn't exist and also assigns
	     * the correct permissions
	     */
	    TableOperations tableOps = connector.tableOperations();
		SecurityOperations secOps = connector.securityOperations();
		
		
		try {
			if (!tableOps.exists(conf.get("sipp.table.name"))) {
				tableOps.create(conf.get("sipp.table.name"));
				
				createTableSplits(tableOps, conf.get("sipp.table.name"));	
			}
			
			// set the correct permissions
			secOps.grantTablePermission(conf.get("principal"), conf.get("sipp.table.name"), TablePermission.READ);
			secOps.grantTablePermission(conf.get("principal"), conf.get("sipp.table.name"), TablePermission.WRITE);
			secOps.grantTablePermission(conf.get("user"), conf.get("sipp.table.name"), TablePermission.READ);
			secOps.grantTablePermission(conf.get("user"), conf.get("sipp.table.name"), TablePermission.WRITE);
			secOps.grantSystemPermission(conf.get("principal"), SystemPermission.OBTAIN_DELEGATION_TOKEN);
			
		} catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
			throw new RuntimeException(e);
		}	
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 *
	 * @param tableOps
	 * @param string
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 * @throws TableNotFoundException 
	 */
	private void createTableSplits(TableOperations tableOps, String string) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
		Configuration conf = getConf();
		
		List<String> splits = Lists.newArrayList(conf.get("sipp.table.splits").split(","));
		SortedSet<Text> setSplits = new TreeSet<Text>();
				
		for(String split : splits) {
			setSplits.add(new Text(split));
		}
		
		tableOps.addSplits(conf.get("sipp.table.name"), setSplits);
		
	}
	

}