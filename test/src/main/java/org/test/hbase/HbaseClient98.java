package org.test.hbase;

import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
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
import org.apache.nutch.crawl.CrawlDatum;
import org.test.hbase.ShortUrlGenerator.IDShorter;

public class HbaseClient98 {
	public static Configuration conf = HBaseConfiguration.create();
	public static String T_CRAWLDBIDX = "crawldbIdx";
	public static String T_CRAWLDBPRE = "crawldb";

	public static void main(String[] args) throws Exception {
		// createCrawldbIdx();
		// createCrawldbs();
		// regionTest();
		// showUrls();
		// getUrl();
		threadInsertData();
		// countTable1();
	}

	public static void countTable1() throws IOException {
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(T_CRAWLDBPRE + 1);
		Scan scan = new Scan();
		scan.setCaching(10000);
		List<Filter> filters = new ArrayList<Filter>();
		Filter filter = new FirstKeyOnlyFilter();
		filters.add(filter);
		FilterList filterList = new FilterList(filters);
		scan.setFilter(filterList);
		ResultScanner rs = table.getScanner(scan);
		long cnt = 0;
		for (Result r : rs) {
			cnt++;
		}
		rs.close();
		table.close();
		connection.close();
		System.out.println("crawldb1 count=" + cnt);
	}

	public static void threadInsertData() throws IOException {
		for (int i = 0; i < 5; i++) {
			new Thread(new Runnable() {
				private long totalCount = 0;
				private long idCount = 0;
				private long idStart = 0;

				public void run() {
					try {
						insertData();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				public void insertData() throws IOException {
					HConnection connection = HConnectionManager.createConnection(conf);
					HTableInterface idx = connection.getTable(T_CRAWLDBIDX);
					HTableInterface data = connection.getTable(T_CRAWLDBPRE + 1);
					idx.setAutoFlush(false, true);
					idx.setWriteBufferSize(12 * 1024 * 1024);
					data.setAutoFlush(false, true);
					data.setWriteBufferSize(12 * 1024 * 1024);

					long start = System.currentTimeMillis();
					for (int i = 0; i < 10000000; i++) {

						int tmp = Long.valueOf((++totalCount) % 2).intValue();
						int scoreIdx = 1;

						String url = "http://www."
								+ ShortUrlGenerator.IDShorter.toShort(RandomUtils.nextLong(), ShortUrlGenerator.chars62)
								+ i + ".com/index.html" + i;
						String id = shortById(url);
						idx.put(getIdxPut(url, id, scoreIdx));
						insertUrls(data, id, url, scoreIdx);

						if ((totalCount % 10000) == 0) {
							System.out.println(Thread.currentThread() + "hdfstotable: " + totalCount + " urls done!");
						}
						if ((totalCount % 200000) == 0) {
							idx.flushCommits();
							data.flushCommits();
							System.out.println("hdfstotable: " + totalCount + " commits.");
						}
					}
					long end = System.currentTimeMillis();
					System.out.println(Thread.currentThread() + "hdfstotable: use=" + (end - start));

					idx.close();
					data.close();
					connection.close();
				}

				private String shortById(String url) {
					if (idCount++ % 10000 == 0) {
						idStart = Long.valueOf(getStartId("10000", 3)).longValue();
					}

					return String.valueOf(idStart++);
				}

				private String getStartId(String count, int retry) {
					String start = null;
					HttpClient httpClient = new DefaultHttpClient();
					httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 10000);
					httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 15000);
					HttpGet httpget = new HttpGet();// Get请求
					List<NameValuePair> qparams = new ArrayList<NameValuePair>();// 设置参数
					qparams.add(new BasicNameValuePair("cnt", count));

					try {
						URI uri = URIUtils.createURI("http", "10.200.6.47", 8080, "/",
								URLEncodedUtils.format(qparams, "UTF-8"), null);
						httpget.setURI(uri);
						// 发送请求
						HttpResponse httpresponse = httpClient.execute(httpget);
						// 获取返回数据
						HttpEntity entity = httpresponse.getEntity();
						String value = EntityUtils.toString(entity);
						if (value != null && !"error".equals(value))
							start = value;
						EntityUtils.consume(entity);
					} catch (Exception e) {
						e.printStackTrace();
						if (retry-- > 0)
							return getStartId(count, retry);
					} finally {
						httpClient.getConnectionManager().shutdown();
					}

					return start;
				}

				private Put getIdxPut(String url, String id, int scoreIdx) throws IOException {
					Put put = new Put(Bytes.toBytes(url));
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("id"), Bytes.toBytes(id));
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Score"), Bytes.toBytes(1f));
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("ScoreIdx"), Bytes.toBytes(scoreIdx));

					put.setDurability(Durability.SKIP_WAL);

					return put;
				}

				private void insertUrls(HTableInterface htable, String shortKey, String url, int scoreIdx)
						throws IOException {
					Put put = new Put(Bytes.toBytes(shortKey));

					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("url"), Bytes.toBytes(url));
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Score"), Bytes.toBytes(1f));
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Status"), new byte[] { CrawlDatum.STATUS_DB_FETCHED });
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Fetchtime"),
							Bytes.toBytes(System.currentTimeMillis() - 360000000));
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Retries"),
							new byte[] { CrawlDatum.STATUS_DB_UNFETCHED });
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("FetchInterval"), Bytes.toBytes(360000000l));

					put.setDurability(Durability.SKIP_WAL);

					htable.put(put);
				}
			}).start();
		}
	}

	public static void showTableProps() throws IOException {
		HConnection connection = HConnectionManager.createConnection(conf);

		HTableInterface table = connection.getTable(T_CRAWLDBIDX);
		System.out.println(table.getTableDescriptor().toString());
		table.close();
		table = connection.getTable(T_CRAWLDBPRE + 100);
		System.out.println(table.getTableDescriptor().toString());
		table.close();

		connection.close();
	}

	public static void getUrl() throws Exception {
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(T_CRAWLDBPRE + 1);

		Get get = new Get("72811071".getBytes());// 根据rowkey查询
		Result r = table.get(get);
		System.out.println("获得到rowkey:" + new String(r.getRow()));
		CrawlDatum datum = new CrawlDatum();
		createValue(datum, r);
		System.out.println(datum);

		table.close();
		connection.close();
	}

	private static void createValue(CrawlDatum datum, Result r) {
		NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("cf1"));
		org.apache.hadoop.io.MapWritable metaData = new org.apache.hadoop.io.MapWritable();
		datum.setMetaData(metaData);

		for (Iterator iterator = map.keySet().iterator(); iterator.hasNext();) {
			byte[] key = (byte[]) iterator.next();
			byte[] value = map.get(key);
			String skey = Bytes.toString(key);

			if ("url".equals(skey)) {
				System.out.println(Bytes.toString(value));
				// nothing
			} else if ("Score".equals(skey)) {
				if (value != null)
					datum.setScore(Bytes.toFloat(value));
			} else if ("Status".equals(skey)) {
				if (value != null)
					datum.setStatus(value[0]);
			} else if ("Fetchtime".equals(skey)) {
				if (value != null)
					datum.setFetchTime(Bytes.toLong(value));
			} else if ("Retries".equals(skey)) {
				if (value != null)
					datum.setRetriesSinceFetch(value[0]);
			} else if ("FetchInterval".equals(skey)) {
				if (value != null)
					datum.setFetchInterval(Bytes.toInt(value));
			} else if ("Modifiedtime".equals(skey)) {
				if (value != null)
					datum.setModifiedTime(Bytes.toLong(value));
			} else if ("Signature".equals(skey)) {
				if (value != null)
					datum.setSignature(value);
			} else {
				metaData.put(new Text(key), new Text(value));
				System.out.println(new Text(key) + "=" + new Text(value));
			}
		}
		metaData.put(new Text("urlid"), new Text(r.getRow()));
	}

	public static void showUrls() throws Exception {
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(T_CRAWLDBPRE + 1);

		Scan scan = new Scan();
		// 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCaching(1000);
		// don't set to true for MR jobs
		// scan.setCacheBlocks(false);
		// 扫描特定区间
		// Scan scan=new Scan(Bytes.toBytes("开始行号"),Bytes.toBytes("结束行号"));

		int i = 0;
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs) {
			if (i++ >= 100)
				break;
			System.out.println("==================================");
			System.out.println("行号:  " + Bytes.toString(r.getRow()));

			byte[] url = r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("url"));
			if (url != null)
				System.out.println("url=" + new String(url));
		}
		rs.close();

		table.close();
		connection.close();
	}

	private static String getHost(String value) {
		String host = null;
		try {
			URL tmp = new URL(value);
			host = tmp.getHost();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		return host;
	}

	private static String getDate(long time) {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time));
	}

	public static void createCrawldbIdx() throws Exception {
		HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf1");
		columnDescriptor.setMaxVersions(1);
		HBaseAdmin admin = new HBaseAdmin(conf);
		createTable(admin, T_CRAWLDBIDX, columnDescriptor, 10737418240l, true, getUrlSplits());
		admin.close();
	}

	public static void createCrawldbs() throws Exception {
		HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf1");
		columnDescriptor.setMaxVersions(1);

		HBaseAdmin admin = new HBaseAdmin(conf);
		// for (int i = 1; i < 4; i++) {
		// deleteTable(admin, T_CRAWLDBPRE + i);
		// }
		for (int i = 1; i < 2; i++) {
			createTable(admin, T_CRAWLDBPRE + i, columnDescriptor, -1, true, getCharSplits("0", "10000000000", 100));
		}
		admin.close();
	}

	public static void regionTest() throws Exception {
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(T_CRAWLDBPRE + 2);
		table.setAutoFlush(false, true);
		for (int j = 0; j < 12; j++) {
			addRow(table, Bytes.toBytes(String.valueOf(j)), Bytes.toBytes("cf1"), Bytes.toBytes("url"),
					Bytes.toBytes("http://www.sina.com/" + j));
			// System.out.println(Bytes.toHex(Bytes.toBytes(Long.valueOf(j))));
		}
		table.flushCommits();
		for (int j = 0; j < 12; j++) {
			System.out.println("region="
					+ connection.getRegionLocation(Bytes.toBytes(T_CRAWLDBPRE + 2), Bytes.toBytes(String.valueOf(j)),
							false));
		}
		table.close();
		connection.close();
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
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		if (maxFileSize != -1)
			tableDescriptor.setMaxFileSize(maxFileSize);
		tableDescriptor.addFamily(columnDescriptor); // 在描述里添加列族
		if (splitKeys != null)
			admin.createTable(tableDescriptor, splitKeys);
		else
			admin.createTable(tableDescriptor);

		System.out.println("创建表成功:" + tableName);
	}

	// 创建数据库表
	public static void createTable(String tableName, HColumnDescriptor[] columnDescriptors, long maxFileSize,
			boolean del) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);

		if (admin.tableExists(tableName)) {
			System.out.println("表已经存在:" + tableName);
			if (del) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println("表已del:" + tableName);
			} else {
				admin.close();
				return;
			}
		} else {
			// 新建一个 表的描述
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			tableDescriptor.setMaxFileSize(maxFileSize);
			for (HColumnDescriptor columnDescriptor : columnDescriptors) {
				tableDescriptor.addFamily(columnDescriptor); // 在描述里添加列族
			}
			admin.createTable(tableDescriptor);
			System.out.println("创建表成功:" + tableName);
		}
		admin.close();
	}

	// 创建数据库表
	public static void createTable(HBaseAdmin admin, String tableName, HColumnDescriptor columnDescriptor,
			long maxFileSize, boolean del, byte[] startKey, byte[] endKey, int numRegions) throws Exception {
		if (admin.tableExists(tableName)) {
			System.out.println("表已经存在:" + tableName);
			if (del) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println("表已del:" + tableName);
			} else {
				admin.close();
				return;
			}
		}
		// 新建一个 scores 表的描述
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		if (maxFileSize != -1)
			tableDescriptor.setMaxFileSize(maxFileSize);
		tableDescriptor.addFamily(columnDescriptor); // 在描述里添加列族
		admin.createTable(tableDescriptor, startKey, endKey, numRegions);
		System.out.println("创建表成功:" + tableName);
	}

	// 添加一条数据 /更新
	public static void addRow(HTableInterface table, byte[] rowId, byte[] cf, byte[] column, byte[] value)
			throws Exception {
		Put put = new Put(rowId);
		// 参数分别：列族、列、值
		put.add(cf, column, value);
		table.put(put);

		// table.setAutoFlushTo(false);
		// table.setAutoFlush(false, true);
		// table.flushCommits();
	}

	public static void deleteColumn(HTableInterface table, String rowkey, String cf, String column) throws Exception {
		Delete del = new Delete(Bytes.toBytes(rowkey));
		del.deleteColumns(Bytes.toBytes(cf), Bytes.toBytes(column));
		table.delete(del);
	}

	/**
	 * 删除一条数据
	 * 
	 * @param tableName
	 *            表名
	 * @param row
	 *            rowkey
	 * **/
	public static void deleteRow(HTableInterface table, String rowkey) throws Exception {
		Delete del = new Delete(Bytes.toBytes(rowkey));
		table.delete(del);// 删除一条数据
	}

	/**
	 * 删除多条数据
	 * 
	 * @param tableName
	 *            表名
	 * @param row
	 *            rowkey
	 * **/
	public static void deleteRows(HTableInterface table, String rowkey[]) throws Exception {
		List<Delete> list = new ArrayList<Delete>();
		for (String k : rowkey) {
			Delete del = new Delete(Bytes.toBytes(k));
			list.add(del);
		}
		table.delete(list);// 删除
	}

	// 删除数据库表
	public static void deleteTable(HBaseAdmin hAdmin, String tableName) throws Exception {
		if (hAdmin.tableExists(tableName)) {
			hAdmin.disableTable(tableName);// 关闭一个表
			hAdmin.deleteTable(tableName); // 删除一个表
			System.out.println("删除表成功:" + tableName);
		} else {
			System.out.println("删除的表不存在:" + tableName);
		}
	}

	/**
	 * 单条件查询,根据rowkey查询唯一一条记录
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void getByKey(HTableInterface table, String key) throws IOException {
		Get scan = new Get(key.getBytes());// 根据rowkey查询
		Result r = table.get(scan);
		System.out.println("获得到rowkey:" + new String(r.getRow()));
		for (Cell cell : r.rawCells()) {
			System.out.println("时间戳:  " + cell.getTimestamp());
			System.out.println("列簇:  "
					+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
			System.out.println("列:  "
					+ Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
			System.out.println("值:  "
					+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
		}
	}

	/**
	 * 单条件按查询，查询多条记录
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void scanFilter(HTableInterface table) throws IOException {
		Scan s = new Scan();
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
				Bytes.toBytes("aaa")); // 当列column1的值为aaa时进行查询
		s.setFilter(filter);

		ResultScanner rs = table.getScanner(s);
		for (Result r : rs) {
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (Cell cell : r.rawCells()) {
				System.out.println("时间戳:  " + cell.getTimestamp());
				System.out.println("列簇:  "
						+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out
						.println("列:  "
								+ Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
										cell.getQualifierLength()));
				System.out.println("值:  "
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		}
		rs.close();
	}

	/**
	 * 组合条件查询
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void scanFilters(HTableInterface table) throws IOException {
		List<Filter> filters = new ArrayList<Filter>();
		Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
				Bytes.toBytes("aaa"));
		filters.add(filter1);
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("column2"), null, CompareOp.EQUAL,
				Bytes.toBytes("bbb"));
		filters.add(filter2);
		Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
				Bytes.toBytes("ccc"));
		filters.add(filter3);
		FilterList filterList1 = new FilterList(filters);

		Scan scan = new Scan();
		scan.setFilter(filterList1);

		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs) {
			System.out.println("获得到rowkey:" + new String(r.getRow()));

			for (Cell cell : r.rawCells()) {
				System.out.println("时间戳:  " + cell.getTimestamp());
				System.out.println("列簇:  "
						+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out
						.println("列:  "
								+ Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
										cell.getQualifierLength()));
				System.out.println("值:  "
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

			}
		}
		rs.close();

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

	public static byte[][] getCharSplits(String startKey, String endKey, int numRegions) {
		byte[][] splits = new byte[numRegions - 1][];
		BigInteger lowestKey = new BigInteger(startKey, 10);
		BigInteger highestKey = new BigInteger(endKey, 10);
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
		lowestKey = lowestKey.add(regionIncrement);
		for (int i = 0; i < numRegions - 1; i++) {
			BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
			byte[] b = String.format("%010d", key).getBytes();
			splits[i] = b;
		}
		return splits;
	}

	public static byte[][] getLongSplits(String startKey, String endKey, int numRegions) {
		byte[][] splits = new byte[numRegions - 1][];
		BigInteger lowestKey = new BigInteger(startKey, 10);
		BigInteger highestKey = new BigInteger(endKey, 10);
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
		lowestKey = lowestKey.add(regionIncrement);
		for (int i = 0; i < numRegions - 1; i++) {
			BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
			// byte[] b = String.format("%010d", key).getBytes();
			byte[] b = Bytes.toBytes(Long.valueOf(key.longValue()));
			splits[i] = b;
		}
		return splits;
	}

	public static byte[][] get62Splits(String startKey, String endKey, int numRegions) {
		byte[][] splits = new byte[numRegions - 1][];
		BigInteger lowestKey = new BigInteger(startKey, 16);
		BigInteger highestKey = new BigInteger(endKey, 16);
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
		lowestKey = lowestKey.add(regionIncrement);
		for (int i = 0; i < numRegions - 1; i++) {
			BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
			byte[] b = IDShorter.toShort(key.longValue(), ShortUrlGenerator.chars62).getBytes();
			splits[i] = b;
		}
		return splits;
	}

	public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
		byte[][] splits = new byte[numRegions - 1][];
		BigInteger lowestKey = new BigInteger(startKey, 16);
		BigInteger highestKey = new BigInteger(endKey, 16);
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
		lowestKey = lowestKey.add(regionIncrement);
		for (int i = 0; i < numRegions - 1; i++) {
			BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
			byte[] b = String.format("%016x", key).getBytes();
			splits[i] = b;
		}
		return splits;
	}
}
