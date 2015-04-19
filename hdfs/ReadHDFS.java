
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class ReadHDFS{
        public static void main (String [] args) throws Exception{
			String path = args[0];
			Configuration conf = new Configuration();
			Path coreSitePath = new Path(System.getenv("HADOOP_HOME"), "conf/core-site.xml");
			conf.addResource(coreSitePath);

			FileSystem hdfs = FileSystem.get(conf);			
			InputStream in = null;
			try {
  				in = hdfs.open(new Path(path));
  				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String line;
				line = br.readLine();
				while(line != null){
					System.out.println(line);
					line = br.readLine();
				}
			} finally {
  				IOUtils.closeStream(in);
		}
	}
}	
