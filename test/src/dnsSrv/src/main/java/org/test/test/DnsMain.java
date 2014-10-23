package org.test.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
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
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

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
		/**
		 * 
		 */
		private static final long serialVersionUID = -2605999282979633805L;
		private Map dns = new HashMap();

		@Override
		public void init() throws ServletException {
			java.security.Security.setProperty("networkaddress.cache.ttl", "-1");

			loadIPs();

			super.init();
		}

		@Override
		public void destroy() {
			super.destroy();
		}

		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			String host = req.getParameter("host");
			if (StringUtils.isEmpty(host)) {
				// resp.getWriter().print();
				// resp.flushBuffer();
				return;
			}

			String action = req.getParameter("action");
			if ("reload".equals(action)) {
				resp.getWriter().print("old size=" + dns.size() + " reloading...");
				resp.flushBuffer();

				loadIPs();

				resp.getWriter().print("new size=" + dns.size());
				resp.flushBuffer();
				return;
			}

			try {
				String ip = null;
				if (dns.containsKey(host))
					ip = (String) dns.get(host);
				else {
					ip = InetAddress.getByName(host).getHostAddress();
					dns.put(host, ip);
				}

				resp.getWriter().print(ip);
				resp.flushBuffer();
			} catch (Exception e) {
				LOG.error(host + " " + e);
			}
		}

		private void loadIPs() {
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
			}
		}

		public static void main(String[] args) throws Exception {

		}

	}

	public static class JettyTest {
		public static void main(String[] args) throws Exception {
			HttpClient httpClient = new DefaultHttpClient();
			httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 10000);
			httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 15000);
			HttpGet httpget = new HttpGet();// Get请求
			List<NameValuePair> qparams = new ArrayList<NameValuePair>();// 设置参数
			qparams.add(new BasicNameValuePair("cnt", "10"));

			try {
				URI uri = URIUtils.createURI("http", "localhost", 8080, "/", URLEncodedUtils.format(qparams, "UTF-8"),
						null);
				httpget.setURI(uri);
				// 发送请求
				HttpResponse httpresponse = httpClient.execute(httpget);
				// 获取返回数据
				HttpEntity entity = httpresponse.getEntity();
				String value = EntityUtils.toString(entity);
				System.out.println(value);
				EntityUtils.consume(entity);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				httpClient.getConnectionManager().shutdown();
			}
		}

		public static void main1(String[] args) throws Exception {
			Server server = new Server(8080);

			ServletContextHandler context0 = new ServletContextHandler(ServletContextHandler.SESSIONS);
			context0.setContextPath("/ctx0");
			// context0.addServlet(new ServletHolder(new HelloServlet()), "/*");
			// context0.addServlet(new ServletHolder(new
			// HelloServlet("buongiorno")), "/it/*");
			// context0.addServlet(new ServletHolder(new
			// HelloServlet("bonjour le Monde")), "/fr/*");

			// WebAppContext webapp = new WebAppContext();
			// String jetty_home = System.getProperty("jetty.home",
			// "F:\\book\\开源项目\\jetty-hightide-8.1.6.v20120903\\jetty-hightide-8.1.6.v20120903");
			// webapp.setContextPath("/ctx1");
			// webapp.setWar(jetty_home + "/webapps/test.war");
			// SecurityHandler securityHandler = new
			// ConstraintSecurityHandler();
			// HashLoginService loginService = new HashLoginService();
			// loginService.setName("Realm");
			// securityHandler.setLoginService(loginService);
			// webapp.setSecurityHandler(securityHandler);

			// ContextHandlerCollection contexts = new
			// ContextHandlerCollection();
			// contexts.setHandlers(new Handler[] { context0, webapp });

			// server.setHandler(contexts);

			server.start();
			server.join();

		}

		public static void main2(String[] args) throws Exception {
			Server server = new Server();
			SelectChannelConnector connector0 = new SelectChannelConnector();
			connector0.setPort(8080);
			connector0.setMaxIdleTime(30000);
			connector0.setRequestHeaderSize(8192);

			SelectChannelConnector connector1 = new SelectChannelConnector();
			connector1.setHost("127.0.0.1");
			connector1.setPort(8888);
			connector1.setThreadPool(new QueuedThreadPool(20));
			connector1.setName("/admin");

			SslSelectChannelConnector ssl_connector = new SslSelectChannelConnector();
			String jetty_home = System.getProperty("jetty.home",
					"F:\\book\\开源项目\\jetty-hightide-8.1.6.v20120903\\jetty-hightide-8.1.6.v20120903");
			System.setProperty("jetty.home", jetty_home);
			ssl_connector.setPort(8443);
			org.eclipse.jetty.util.ssl.SslContextFactory cf = ssl_connector.getSslContextFactory();
			cf.setKeyStorePath(jetty_home + "/etc/keystore");
			cf.setKeyStorePassword("OBF:1vny1zlo1x8e1vnw1vn61x8g1zlu1vn4");
			cf.setKeyManagerPassword("OBF:1u2u1wml1z7s1z7a1wnl1u2g");

			server.setConnectors(new Connector[] { connector0, connector1, ssl_connector });

			// server.setHandler(new HelloHandler());

			server.start();
			server.join();
		}
	}
}
