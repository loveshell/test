package org.test.hadoop;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 提供ip黑名单的过滤，广告url的过滤
 * 
 * @author xxhuang
 */
public class UrlFilters {
	public static final Logger LOG = LoggerFactory.getLogger(UrlFilters.class);

	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	public static SimpleDateFormat dateSdf = new SimpleDateFormat("yyyyMMdd");
	public static final byte HOST_BLACK = 1;
	public static final byte HOST_UNKNOW = 2;
	public static final byte HOST_WHITE = 3;
	public static final String delimiter = "●";

	private final static String FETCHFAIL_NAME = "fetchFail";
	private final static String IPBLACK_NAME = "IPBlack";
	private static int tableCacheSize = 50000;
	private static int statsCommit = 500000;
	private static long saveErrorCnt = 0l;

	private static Map blackIPs = new HashMap();
	private static Map whiteHosts = new HashMap();
	protected static BlockingQueue<String[]> errorUrls = new LinkedBlockingQueue();
	private static AtomicBoolean errorToken = new AtomicBoolean(false);
	private static AtomicBoolean ipLoad = new AtomicBoolean(false);
	private static Map tables = new HashMap();

	static {
		java.security.Security.setProperty("networkaddress.cache.ttl", "-1");

		try {
			HConnection connection = HConnectionManager.createConnection(HBaseConfiguration.create());
			HTableInterface fetchFailTable = initTable(connection, FETCHFAIL_NAME);
			HTableInterface ipBlackTable = initTable(connection, IPBLACK_NAME);
			tables.put("conn", connection);
			tables.put(FETCHFAIL_NAME, fetchFailTable);
			tables.put(IPBLACK_NAME, ipBlackTable);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class HttpConnectionManager {
		private static HttpClient httpClient;
		private static String host = "localhost";
		private static int port = 8080;
		/**
		 * 最大连接数
		 */
		public final static int MAX_TOTAL_CONNECTIONS = 800;
		/**
		 * 每个路由最大连接数
		 */
		public final static int MAX_ROUTE_CONNECTIONS = 400;
		/**
		 * 连接超时时间
		 */
		public final static int CONNECT_TIMEOUT = 2000;
		/**
		 * 读取超时时间
		 */
		public final static int READ_TIMEOUT = 3000;

		static {
			SchemeRegistry registry = new SchemeRegistry();
			registry.register(new Scheme("http", port, PlainSocketFactory.getSocketFactory()));
			registry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));
			ThreadSafeClientConnManager connectionManager = new ThreadSafeClientConnManager(registry);
			// 设置最大连接数
			connectionManager.setMaxTotal(MAX_TOTAL_CONNECTIONS);
			connectionManager.setDefaultMaxPerRoute(MAX_ROUTE_CONNECTIONS);

			HttpParams httpParams = new BasicHttpParams();
			// 设置连接超时时间
			HttpConnectionParams.setConnectionTimeout(httpParams, CONNECT_TIMEOUT);
			// 设置读取超时时间
			HttpConnectionParams.setSoTimeout(httpParams, READ_TIMEOUT);

			httpClient = new DefaultHttpClient(connectionManager, httpParams);

			Configuration conf = NutchConfiguration.create();
			host = conf.get("id.server.name");
			port = conf.getInt("id.server.port", 8080);
		}

		public static HttpClient getHttpClient() {
			return httpClient;
		}

		public static HttpGet getHttpGet(NameValuePair pair) throws Exception {
			HttpGet httpget = new HttpGet();// Get请求
			List<NameValuePair> qparams = new ArrayList<NameValuePair>();// 设置参数
			qparams.add(pair);
			httpget.setURI(URIUtils.createURI("http", host, port, "/", URLEncodedUtils.format(qparams, "UTF-8"), null));

			return httpget;
		}
	}

	private static HTableInterface initTable(HConnection connection, String tableName) throws Exception {
		HTableInterface table = connection.getTable(tableName);
		table.setAutoFlush(false, true);
		table.setWriteBufferSize(500 * tableCacheSize);

		return table;
	}

	/**
	 * 不支持多线程
	 * 
	 * @return
	 * @throws Exception
	 */
	private static HConnection getConn() throws Exception {
		String key = "conn";
		if (tables.containsKey(key)) {
			HConnection connection = (HConnection) tables.get(key);
			if (connection == null) {
				connection = HConnectionManager.createConnection(HBaseConfiguration.create());
				tables.put(key, connection);
			} else {
				if (connection.isClosed()) {
					connection = HConnectionManager.createConnection(HBaseConfiguration.create());
					tables.put(key, connection);
				}
			}
		} else {
			HConnection connection = HConnectionManager.createConnection(HBaseConfiguration.create());
			tables.put(key, connection);
		}

		return (HConnection) tables.get(key);
	}

	private static HTableInterface getTable(String name) throws Exception {
		if (tables.containsKey(name)) {
			HTableInterface table = (HTableInterface) tables.get(name);
			if (table == null) {
				tables.put(name, initTable(getConn(), name));
			}
		} else {
			tables.put(name, initTable(getConn(), name));
		}

		return (HTableInterface) tables.get(name);
	}

	public static String getIP(String host) {
		HttpClient httpClient = HttpConnectionManager.getHttpClient();

		HttpEntity entity = null;
		try { // 发送请求
			HttpResponse httpresponse = httpClient.execute(HttpConnectionManager.getHttpGet(new BasicNameValuePair(
					"host", host)));
			// 获取返回数据
			entity = httpresponse.getEntity();
			return EntityUtils.toString(entity);
		} catch (Exception e) {
			LOG.error(e.toString());
			try {
				return InetAddress.getByName(host).getHostAddress();
			} catch (UnknownHostException e1) {
				return null;
			}
		} finally {
			if (entity != null)
				try {
					EntityUtils.consume(entity);
				} catch (IOException e) {
					e.printStackTrace();
				}
			// httpClient.getConnectionManager().shutdown();
		}
	}

	// for parse
	public static byte getHostType(String url) {
		loadIPs();

		String host = getHost(url);
		if (StringUtils.isNotEmpty(host)) {
			String ip = getIP(host);
			if (StringUtils.isEmpty(ip))
				return HOST_BLACK;
			if (blackIPs.containsKey(ip))// 白名单host前面已经pass了
				return HOST_BLACK;
		} else
			return HOST_BLACK;

		// TODO AD

		return HOST_UNKNOW;
	}

	/**
	 * 加载白名单host，黑名单ip, ip cache
	 */
	public static void loadIPs() {
		if (!ipLoad.compareAndSet(false, true)) {
			return;
		}

		try {
			HTableInterface ipBlackTable = getTable(IPBLACK_NAME);
			Scan scan = new Scan();
			scan.setCaching(tableCacheSize);

			ResultScanner rs = ipBlackTable.getScanner(scan);
			for (Result r : rs) {
				String ip = Bytes.toString(r.getRow());
				if (ip != null) {
					blackIPs.put(ip, null);
				}
			}
			rs.close();
			ipBlackTable.close();
			LOG.info("load black ips=" + blackIPs.size());

			getConn().close();

			whiteHosts.put("stock.cnstock.com", null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String getHost(String url) {
		String host = null;
		try {
			URL tmp = new URL(url);
			host = tmp.getHost();
		} catch (MalformedURLException e) {
			// e.printStackTrace();
		}
		return host;
	}

	// for fetch
	public static void saveError(String[] errorUrl) {
		errorUrls.add(errorUrl);
		if (errorUrls.size() > 10000 && errorToken.compareAndSet(false, true)) {
			Set<String[]> set = new HashSet();
			int i = 0;
			while (i++ < 10000) {
				set.add(errorUrls.poll());
			}

			for (String[] object : set) {
				saveError(object[0], object[1], object[2]);
			}

			errorToken.compareAndSet(true, false);
		}
	}

	// String s = getClass().getName();
	// String message = getLocalizedMessage();
	// return (message != null) ? (s + ": " + message) : s;
	private static void saveError(String url, String msg, String ip) {
		// if (StringUtils.isEmpty(msg))
		// return;
		String host = getHost(url);
		// if (StringUtils.isEmpty(host))
		// return;
		// TODO 加host白名单
		if (whiteHosts.containsKey(host))
			return;
		// no ip,parse直接处理
		// if (msg.indexOf(UnknownHostException.class.getName()) > -1)
		// return;

		try {
			String time = sdf.format(new Date());

			Put put = new Put(Bytes.toBytes(host + delimiter + time));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("time"), Bytes.toBytes(Long.valueOf(time)));
			// new ip from fetch overwrite old in table
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("ip"), Bytes.toBytes(ip));
			String[] tmp = msg.split(":");
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("exception"), Bytes.toBytes(tmp[0]));
			if (tmp.length > 1 && StringUtils.isNotEmpty(tmp[1]))
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("msg"), Bytes.toBytes(tmp[1]));

			HTableInterface fetchFailTable = getTable(FETCHFAIL_NAME);
			fetchFailTable.put(put);

			if (++saveErrorCnt % tableCacheSize == 0) {
				fetchFailTable.flushCommits();
			}
		} catch (Exception e) {
			e.printStackTrace();
			// connection = null;
		}
	}

	// fetch phase
	public static void closeFetchFailTable() {
		LOG.info("closeFetchFailTable: start");
		try {
			HTableInterface fetchFailTable = getTable(FETCHFAIL_NAME);

			Object[] obj = errorUrls.toArray();
			for (int i = 0; i < obj.length; i++) {
				String[] tmp = (String[]) obj[i];
				saveError(tmp[0], tmp[1], tmp[2]);
			}

			fetchFailTable.flushCommits();
			fetchFailTable.close();
			getConn().close();

			LOG.info("closeFetchFailTable: end");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			help();
			return;
		}
		for (int i = 0; i < args.length; i++) {
			if ("--urlBlackClean".equals(args[i])) {
				urlBlackClean();
			} else if ("--urlStatusDel".equals(args[i])) {
				if (i == args.length - 1)
					urlStatusDel(3);
				else
					urlStatusDel(Integer.parseInt(args[++i]));
			} else if ("--removeWhiteFromBlack".equals(args[i])) {
				removeWhiteFromBlack();
			} else if ("--extractBlackIP".equals(args[i])) {
				if (i == args.length - 1)
					extractBlackIP(SocketTimeoutException.class.getName());
				else
					extractBlackIP(args[++i]);
			} else if ("--statsFetchErrors".equals(args[i])) {
				statsFetchErrors();
			} else if ("--createFetchFail".equals(args[i])) {
				createFetchFail();
			} else if ("--createIPBlack".equals(args[i])) {
				createIPBlack();
			} else {
				help();
			}
		}
	}

	public static void help() {
		System.out.println("useage: --method var");
	}

	/**
	 * 清理url索引表、url内容表中的黑ip记录 ip解析慢
	 * 
	 * @throws Exception
	 */
	public static void urlBlackClean() throws Exception {
		loadIPs();

		LOG.info("start urlBlackClean ......");

		HConnection connection = HConnectionManager.createConnection(HBaseConfiguration.create());
		HTableInterface idxTable = connection.getTable("crawldbIdx");
		idxTable.setAutoFlush(false, true);
		idxTable.setWriteBufferSize(500 * tableCacheSize);

		Scan scan = new Scan();
		scan.setCaching(tableCacheSize);

		List<Filter> filters = new ArrayList<Filter>();
		Filter filter = new FirstKeyOnlyFilter();
		filters.add(filter);

		FilterList filterList = new FilterList(filters);
		scan.setFilter(filterList);
		ResultScanner rs = idxTable.getScanner(scan);

		long cnt = 0;
		long p = 0;
		List<Delete> keys = new ArrayList<Delete>();
		for (Result r : rs) {
			if (++p % (tableCacheSize) == 0) {
				LOG.info("crawldbIdx scan=" + p);
			}

			String url = Bytes.toString(r.getRow());
			String host = getHost(url);
			try {
				String ip = getIP(host);
				if (StringUtils.isNotEmpty(ip) && !blackIPs.containsKey(ip)) {
					// System.out.println(ip);
					continue;
				}
			} catch (Exception e) {
				// error go del
			}

			keys.add(new Delete(r.getRow()));

			if (++cnt % (tableCacheSize) == 0) {
				idxTable.delete(keys);
				idxTable.flushCommits();
				keys = new ArrayList<Delete>();

				System.out.println("urlBlackClean crawldbIdx host=" + host + " current=" + cnt);
			}
		}
		idxTable.delete(keys);
		idxTable.flushCommits();

		LOG.info("urlBlackClean crawldbIdx total=" + cnt);
		rs.close();
		idxTable.close();

		// public static final byte STATUS_DB_GONE = 0x03;
		for (int i = 1; i <= 3; i++) {
			HTableInterface table = connection.getTable("crawldb" + i);
			table.setAutoFlush(false, true);
			table.setWriteBufferSize(500 * tableCacheSize);

			rs = table.getScanner(scan);

			cnt = 0;
			p = 0;
			keys = new ArrayList<Delete>();
			for (Result r : rs) {
				if (++p % (tableCacheSize) == 0) {
					LOG.info("crawldb" + i + " scan=" + p);
				}

				String host = Bytes.toString(r.getRow());
				String head = "";
				int index = host.lastIndexOf('.');
				if (index > -1) {
					head = host.substring(0, index);
					host = host.substring(index);
				}
				char[] tmp = host.toCharArray();
				for (int j = 0; j < tmp.length; j++) {
					if (Character.isDigit(tmp[j])) {
						host = (head + host).substring(0, j);
						break;
					}
				}

				try {
					String ip = getIP(host);
					if (StringUtils.isNotEmpty(ip) && !blackIPs.containsKey(ip))
						continue;
				} catch (Exception e) {
					// go
				}

				keys.add(new Delete(r.getRow()));
				if (++cnt % (tableCacheSize) == 0) {
					table.delete(keys);
					table.flushCommits();
					keys = new ArrayList<Delete>();

					System.out.println("urlBlackClean " + Bytes.toString(table.getTableName()) + " host=" + host
							+ " current=" + cnt);
				}
			}
			table.delete(keys);
			table.flushCommits();

			LOG.info("urlBlackClean " + Bytes.toString(table.getTableName()) + " total=" + cnt);
			rs.close();

			table.close();
		}
		connection.close();
	}

	// for clean
	/**
	 * 按状态清理url内容表中的记录
	 * 
	 * @param status
	 * @throws Exception
	 */
	public static void urlStatusDel(int status) throws Exception {
		System.out.println("start deleteRows ...... status=" + status);
		// public static final byte STATUS_DB_GONE = 0x03;
		HConnection connection = HConnectionManager.createConnection(HBaseConfiguration.create());

		Scan scan = new Scan();
		scan.setCaching(statsCommit);

		List<Filter> filters = new ArrayList<Filter>();
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("Status"), CompareOp.EQUAL,
				new byte[] { Bytes.toBytes(status)[3] });
		filters.add(filter);
		filter = new FirstKeyOnlyFilter();
		filters.add(filter);

		FilterList filterList = new FilterList(filters);
		scan.setFilter(filterList);

		for (int i = 1; i <= 3; i++) {
			HTableInterface table = connection.getTable("crawldb" + i);
			table.setAutoFlush(false, true);
			table.setWriteBufferSize(500 * statsCommit);

			ResultScanner rs = table.getScanner(scan);

			long cnt = 0;
			List<Delete> keys = new ArrayList<Delete>();
			for (Result r : rs) {
				keys.add(new Delete(r.getRow()));
				if (++cnt % statsCommit == 0) {
					table.delete(keys);
					table.flushCommits();
					keys.clear();

					System.out.println("urlStatusDel " + Bytes.toString(table.getTableName()) + " current="
							+ Bytes.toString(r.getRow()) + " cnt=" + cnt);
				}
			}
			table.delete(keys);
			table.flushCommits();

			System.out.println("urlStatusDel " + Bytes.toString(table.getTableName()) + " total=" + cnt);
			rs.close();

			table.close();
		}
		connection.close();
	}

	// 异常表、ip黑名单表
	/**
	 * 按白名单host，清理异常表、ip黑名单表中的记录
	 * 
	 * @throws Exception
	 */
	public static void removeWhiteFromBlack() throws Exception {
		loadIPs();

		HConnection connection = HConnectionManager.createConnection(HBaseConfiguration.create());
		HTableInterface table = connection.getTable(FETCHFAIL_NAME);
		table.setAutoFlush(false, true);
		table.setWriteBufferSize(500 * statsCommit);
		HTableInterface table1 = connection.getTable(IPBLACK_NAME);
		table1.setAutoFlush(false, true);
		table1.setWriteBufferSize(500 * statsCommit);

		Scan scan = new Scan();
		scan.setCaching(statsCommit);
		for (Iterator iterator = whiteHosts.keySet().iterator(); iterator.hasNext();) {
			String host = (String) iterator.next();

			scan.setStartRow(Bytes.toBytes(host));
			scan.setStopRow(Bytes.toBytes(host + delimiter + delimiter));

			ResultScanner rs = table.getScanner(scan);

			long cnt = 0;
			List<Delete> keys = new ArrayList<Delete>();
			Set<Delete> ips = new HashSet<Delete>();
			for (Result r : rs) {
				keys.add(new Delete(r.getRow()));
				NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("cf"));
				byte[] value = map.get(Bytes.toBytes("ip"));
				if (value != null) {
					ips.add(new Delete(value));
				}
				if (++cnt % statsCommit == 0) {
					table.delete(keys);
					table.flushCommits();
					keys.clear();

					List<Delete> tmp = new ArrayList();
					tmp.addAll(ips);
					table1.delete(tmp);
					table1.flushCommits();
					ips.clear();

					System.out.println("removeWhiteFromBlack current=" + Bytes.toString(r.getRow()) + " cnt=" + cnt);
				}
				System.out
						.println("removeWhiteFromBlack host=" + Bytes.toString(r.getRow()) + " ip=" + value == null ? ""
								: Bytes.toString(value));
			}
			table.delete(keys);
			table.flushCommits();

			List<Delete> tmp = new ArrayList();
			tmp.addAll(ips);
			table1.delete(tmp);
			table1.flushCommits();

			System.out.println("removeWhiteFromBlack total=" + cnt);
			rs.close();
		}

		table.close();
		table1.close();
		connection.close();
	}

	// for insert ips
	/**
	 * 从异常表中检索ip注入ip黑名单
	 * 
	 * @param err
	 * @throws Exception
	 */
	public static void extractBlackIP(String err) throws Exception {
		IPv4Util.loadIPRecords(new Configuration());

		HTableInterface fetchFailTable = getTable(FETCHFAIL_NAME);
		HTableInterface ipBlackTable = getTable(IPBLACK_NAME);

		Scan scan = new Scan();
		scan.setCaching(statsCommit);

		List<Filter> filters = new ArrayList<Filter>();
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("exception"), CompareOp.EQUAL,
				Bytes.toBytes(err));
		filters.add(filter);
		filter = new ColumnPrefixFilter(Bytes.toBytes("ip"));
		filters.add(filter);

		FilterList filterList = new FilterList(filters);
		scan.setFilter(filterList);
		ResultScanner rs = fetchFailTable.getScanner(scan);

		long cnt = 0;
		Map<String, AtomicLong> passedIP = new HashMap();
		for (Result r : rs) {
			NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("cf"));
			String value = Bytes.toString(map.get(Bytes.toBytes("ip")));
			if (value != null && !IPv4Util.isChinaIP(value)) {
				ipBlackTable.put(makeIPPut(value));
			} else {
				String host = Bytes.toString(r.getRow()).split(delimiter)[0];
				if (passedIP.containsKey(value + delimiter + host)) {
					passedIP.get(value + delimiter + host).incrementAndGet();
				} else {
					AtomicLong tmp = new AtomicLong(1);
					passedIP.put(value + delimiter + host, tmp);
				}
				continue;
			}
			if (++cnt % statsCommit == 0) {
				ipBlackTable.flushCommits();
				System.out.println("extractBlackIP url=" + Bytes.toString(r.getRow()) + " cnt=" + cnt);
			}
		}
		rs.close();

		System.out.println("errType=" + err + " total url=" + cnt);
		System.out.println("passedIP=" + passedIP.size());
		System.out.println(passedIP);

		ipBlackTable.flushCommits();
		ipBlackTable.close();
		fetchFailTable.close();
		getConn().close();
	}

	private static Put makeIPPut(String ip) {
		Put put = new Put(Bytes.toBytes(ip));
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("date"), Bytes.toBytes(Long.valueOf(dateSdf.format(new Date()))));
		return put;
	}

	/**
	 * 
	 * 统计异常表中的错误类型、及总记录、ip记录
	 * 
	 * @throws Exception
	 */
	public static void statsFetchErrors() throws Exception {
		HTableInterface fetchFailTable = getTable(FETCHFAIL_NAME);

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) + 1);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);

		Scan scan = new Scan();
		scan.setCaching(statsCommit);
		// List<Filter> filters = new ArrayList<Filter>();
		// Filter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"),
		// Bytes.toBytes("time"),
		// CompareOp.LESS_OR_EQUAL,
		// Bytes.toBytes(Long.valueOf(sdf.format(calendar.getTime()))));
		// filters.add(filter);
		// filter = new FirstKeyOnlyFilter();
		// filters.add(filter);
		//
		// FilterList filterList = new FilterList(filters);
		// scan.setFilter(filterList);
		ResultScanner rs = fetchFailTable.getScanner(scan);

		long cnt = 0;
		Set ips = new HashSet();
		Map errors = new HashMap();
		for (Result r : rs) {
			NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("cf"));
			byte[] value = map.get(Bytes.toBytes("ip"));
			if (value != null) {
				ips.add(Bytes.toString(value));
			}
			byte[] er = map.get(Bytes.toBytes("exception"));
			if (er != null) {
				String erStr = Bytes.toString(er).trim();
				if (errors.containsKey(erStr)) {
					((AtomicLong) errors.get(erStr)).incrementAndGet();
				} else {
					AtomicLong tmp = new AtomicLong(1);
					errors.put(erStr, tmp);
				}
			}

			if (++cnt % statsCommit == 0) {
				System.out.println("statsFetchErrors host=" + Bytes.toString(r.getRow()) + " cnt=" + cnt);
			}
		}
		rs.close();

		System.out.println("total host=" + cnt + "total ip=" + ips.size());
		System.out.println(errors);

		fetchFailTable.close();
		getConn().close();
	}

	// host+●+time,time,ip,exception,msg
	public static void createFetchFail() throws Exception {
		HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf");
		columnDescriptor.setMaxVersions(1);
		// setDurability
		columnDescriptor.setValue("DURABILITY", Durability.ASYNC_WAL.name());
		columnDescriptor.setValue(HTableDescriptor.DEFERRED_LOG_FLUSH, "true");
		// columnDescriptor.setCompressionType(Algorithm.SNAPPY);
		HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
		createTable(admin, FETCHFAIL_NAME, columnDescriptor, 10737418240l, true, getUrlSplits());
		admin.close();

		System.out.println("createFetchFail: end.");
	}

	// ip,date，地区？
	public static void createIPBlack() throws Exception {
		HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf");
		columnDescriptor.setMaxVersions(1);
		// setDurability
		columnDescriptor.setValue("DURABILITY", Durability.ASYNC_WAL.name());
		columnDescriptor.setValue(HTableDescriptor.DEFERRED_LOG_FLUSH, "true");
		// columnDescriptor.setCompressionType(Algorithm.SNAPPY);
		HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
		createTable(admin, IPBLACK_NAME, columnDescriptor, 10737418240l, true, null);
		admin.close();

		System.out.println("createIPBlack: end.");
	}

	// 创建数据库表
	public static void createTable(HBaseAdmin admin, String tableName, HColumnDescriptor columnDescriptor,
			long maxFileSize, boolean del, byte[][] splitKeys) throws Exception {
		if (admin.tableExists(tableName)) {
			System.out.println("表已经存在:" + tableName);
			if (del) {
				if (admin.isTableAvailable(tableName))
					admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println("表已del:" + tableName);
			} else {
				admin.close();
				return;
			}
		}
		// 新建一个 表的描述
		HTableDescriptor tableDescriptor = new HTableDescriptor((tableName));
		if (maxFileSize != -1)
			tableDescriptor.setMaxFileSize(maxFileSize);
		tableDescriptor.addFamily(columnDescriptor); // 在描述里添加列族
		if (splitKeys != null)
			admin.createTable(tableDescriptor, splitKeys);
		else
			admin.createTable(tableDescriptor);

		System.out.println("创建表成功:" + tableName);
	}

	public static byte[][] getUrlSplits() {
		int numRegions = 27;
		String first = "http://a";
		String middlePre = "http://www.";
		char a = 'a';
		byte[][] splits = new byte[numRegions][];
		splits[0] = first.getBytes();
		for (int i = 1; i < numRegions; i++) {
			StringBuilder sb = new StringBuilder(middlePre).append(a++);
			byte[] b = sb.toString().getBytes();
			splits[i] = b;
			// System.out.println(sb.toString());
		}
		return splits;
	}
}
