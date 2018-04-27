package conf;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ClientConfig extends Properties {

	private static final long serialVersionUID = 1L;

	public ClientConfig(String[] args) {
		try {
			this.load(new FileReader(args[0]));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String user() {
		return getProperty("user");
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
