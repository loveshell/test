package org.test.hadoop;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class JobTest {
	public static void main(String[] args) throws Exception {
		MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

		ObjectName name = new ObjectName("hadoop:service=JobTracker,name=JobTrackerInfo");

		// JobTracker jobTracker = (JobTracker)
		// mBeanServer.getMBeanInfo(name).g;
	}

}
