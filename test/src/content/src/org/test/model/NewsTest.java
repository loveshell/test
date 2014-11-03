package org.test.model;

import java.io.File;
import java.util.logging.Logger;


//export runable jar
//nohup java -jar NewsTest1.jar /data/hwei/newsFetcher/bigtest/result_final/result/ &
//nohup java -jar NewsTest.jar /opt/software/job/result20141021/ > do.log &

public class NewsTest {
	private static GetContent getContent = new GetContent();
	private static Logger logger = Logger.getLogger("s");

	public static void main(String[] args) {
		File[] files = new File(args[0]).listFiles();
		for (int i = 0; i < files.length; i++) {
			logger.info("-------------------------  " + files[i].getName() + " Site Start !-------------------------");
			if (new File(files[i].getAbsolutePath() + "/news/").listFiles().length == 0) {
				logger.info("-------------------------  " + files[i].getName() + " no file !-------------------------");
				continue;
			}
			getContent.patternTest(files[i].getAbsolutePath(), 10, 15);
			logger.info("-------------------------  " + files[i].getName() + " Site end ! -------------------------");
		}
	}
}
