package org.test.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Main application class for Hush - The HBase URL Shortener.
 */
public class DnsMain {
	private final static Log LOG = LogFactory.getLog(DnsMain.class);

	/**
	 * Main entry point to application. Sets up the resources and launches the
	 * Jetty server.
	 * 
	 * @param args
	 *            The command line arguments.
	 * @throws Exception
	 *             When there is an issue launching the application.
	 */
	public static void main(String[] args) {
		// set up command line options
		Options options = new Options();
		options.addOption("p", "port", true, "Port to bind to [default: 8080]");

		// parse command line parameters
		CommandLine commandLine = null;
		try {
			commandLine = new PosixParser().parse(options, args);
		} catch (ParseException e) {
			LOG.error("Could not parse command line args: ", e);
			printUsageAndExit(options, -1);
		}

		int port = 8080;
		// user provided value precedes config value
		if (commandLine != null && commandLine.hasOption("port")) {
			String val = commandLine.getOptionValue("port");
			// get port to bind to
			port = Integer.parseInt(val);
			LOG.debug("Port set to: " + val);
		}

		LOG.info("DNS Web server setup.");
		// create server and configure basic settings
		Server server = new Server();
		server.setStopAtShutdown(true);
		// set up connector
		Connector connector = new SelectChannelConnector();
		connector.setPort(port);
		// connector.setHost("127.0.0.1");
		server.addConnector(connector);

		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		server.setHandler(context);
		context.addServlet(new ServletHolder(new DnsServ()), "/*");
		// context.addServlet(new ServletHolder(new
		// HelloServlet("TYPE1 Request")), "/TYPE1/*");

		// start the server
		try {
			server.start();
			server.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Helper method to print out the command line arguments available.
	 * 
	 * @param options
	 *            The command line argument definition.
	 * @param exitCode
	 *            The exit code to use when exiting the application.
	 */
	private static void printUsageAndExit(Options options, int exitCode) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("UrlIdMain", options, true);
		System.exit(exitCode);
	}

	public static class DnsServ extends HttpServlet {

		private static final long serialVersionUID = -2605999282979633805L;

		private Map dns;
		private Map unknownHosts;
		private Map querying;

		private LinkedBlockingQueue queryQueue = new LinkedBlockingQueue(100000);
		private static int queryConcurrent = 3;
		private final static String ipPend = ".";
		private final static String ipUnknown = "u";
		// 限制线程并发数
		private ExecutorService executorService = Executors.newFixedThreadPool(queryConcurrent,
				new ThreadFactoryBuilder().setNameFormat("dns query-%d").setDaemon(true).build());

		@Override
		public void init() throws ServletException {
			java.security.Security.setProperty("networkaddress.cache.ttl", "-1");
			// java.security.Security.setProperty("networkaddress.cache.negative.ttl",
			// "60");// second

			loadIPs();

			startQueryThread();

			super.init();
		}

		@Override
		public void destroy() {
			executorService.shutdown();
			super.destroy();
		}

		private static boolean isLegalHost(String host) {
			if (StringUtils.isEmpty(host))
				return false;
			if (host.length() > 60)
				return false;
			if (host.startsWith(".") || host.startsWith("-"))
				return false;

			host = host.replaceAll("-", "");
			host = host.replaceAll("\\.", "");
			if (!isAsciiAlphanumeric(host))
				return false;

			return true;
		}

		private static boolean isAsciiAlphanumeric(String str) {
			if (StringUtils.isEmpty(str)) {
				return false;
			}
			int sz = str.length();
			for (int i = 0; i < sz; i++) {
				if (CharUtils.isAsciiAlphanumeric(str.charAt(i)) == false) {
					return false;
				}
			}
			return true;
		}

		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			String host = req.getParameter("host");
			if (!isLegalHost(host)) {
				LOG.error("from=" + req.getRemoteAddr() + "errHost=" + host);
				return;
			}

			String action = req.getParameter("action");
			if ("reload".equals(action)) {
				resp.getWriter().println("old size=" + dns.size() + " reloading...");
				resp.flushBuffer();

				loadIPs();

				resp.getWriter().println("new size=" + dns.size());
				resp.flushBuffer();
				return;
			} else if ("count".equals(action)) {
				resp.getWriter().println("dns=" + dns.size());
				resp.getWriter().println("unknownHosts=" + unknownHosts.size());
				resp.getWriter().println("querying=" + querying.size());
				resp.flushBuffer();
				return;
			} else if ("cleanUnknown".equals(action)) {// 清理 unknownhost缓存
				resp.getWriter().println("unknownHosts=" + unknownHosts.size());
				resp.flushBuffer();
				unknownHosts = new HashMap(1000000);
				return;
			}

			try {
				if (dns.containsKey(host)) {
					resp.getWriter().print(dns.get(host));
					resp.flushBuffer();
					return;
				} else if (unknownHosts.containsKey(host)) {
					resp.getWriter().print(ipUnknown);
					resp.flushBuffer();
					return;
				} else {// 直接返回，查找
					queryQueue.add(host);// doing

					resp.getWriter().print(ipPend);// 不确定
					resp.flushBuffer();
					return;
				}
			} catch (Exception e) {
				LOG.error("from=" + e.toString());

				resp.getWriter().print(ipPend);
				resp.flushBuffer();
				return;
			}
		}

		/**
		 * 查询入口，控制同host只能查一次
		 */
		private void startQueryThread() {
			Thread t = new Thread() {
				public void run() {
					Thread.currentThread().setName("dnsQuery");
					LOG.info(Thread.currentThread() + ": 线程启动");
					while (true) {
						try {
							if (queryQueue.isEmpty()) {
								Thread.sleep(1000);
							} else {
								Set set = new HashSet();
								// 保证这批次host唯一
								queryQueue.drainTo(set);

								for (Object object : set) {
									String host = (String) object;
									// 注意先后，不会有多线程问题
									// 上批次的用querying控制
									if (!(querying.containsKey(host) || dns.containsKey(host) || unknownHosts
											.containsKey(host))) {
										querying.put(host, null);
										dnsQuery(host);
									}
								}

								Thread.sleep(2000);
							}
						} catch (Exception e) {
							LOG.error(e.toString());
						}
					}
				}
			};
			t.setDaemon(true);
			t.start();
		}

		private void dnsQuery(final String host) {
			try {
				executorService.submit(new Runnable() {
					public void run() {
						String ip;
						try {
							ip = InetAddress.getByName(host).getHostAddress();
							dns.put(host, ip);
							querying.remove(host);// 注意先后，不会有多线程问题
						} catch (UnknownHostException e) {
							unknownHosts.put(host, null);
							querying.remove(host);

							LOG.error(e.toString());
						}
					}
				});
			} catch (Exception e) {
				LOG.error(e.toString());
			}
		}

		private void loadIPs() {
			dns = new HashMap(100000000);// 无需并发
			unknownHosts = new HashMap(1000000);
			querying = new HashMap(100000);

			try {
				int statsCommit = 500000;

				HConnection connection = HConnectionManager.createConnection(HBaseConfiguration.create());
				HTableInterface fetchFailTable = connection.getTable("fetchFail");
				Scan scan = new Scan();
				scan.setCaching(statsCommit);

				List<Filter> filters = new ArrayList<Filter>();
				Filter filter = new ColumnPrefixFilter(Bytes.toBytes("ip"));
				filters.add(filter);
				FilterList filterList = new FilterList(filters);
				scan.setFilter(filterList);

				ResultScanner rs = fetchFailTable.getScanner(scan);
				long cnt = 0;
				for (Result r : rs) {
					NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("cf"));
					String ip = Bytes.toString(map.get(Bytes.toBytes("ip")));
					String host = Bytes.toString(r.getRow()).split("●")[0];
					if (host != null && ip != null) {
						dns.put(host, ip);
					}

					if (++cnt % statsCommit == 0) {
						LOG.info("loadIPs url=" + Bytes.toString(r.getRow()) + " cnt=" + cnt);
					}
				}
				rs.close();
				fetchFailTable.close();
				LOG.info("load hostip cache=" + dns.size());

				connection.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				//
			}
		}
	}
}
