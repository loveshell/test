package hbase;

import hbase.ShortUrlGenerator.IDShorter;

import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

public class HbaseClient {
	public static Configuration conf = HBaseConfiguration.create();
	public static String T_CRAWLDBIDX = "crawldbIdx";
	public static String T_CRAWLDBPRE = "crawldb";

	@InterfaceAudience.Public
	@InterfaceStability.Stable
	public static class HostFilter extends FilterBase {
		private int hostLimit = 50;

		protected byte[] columnFamily = Bytes.toBytes("cf1");
		protected byte[] columnQualifier = Bytes.toBytes("url");
		private boolean found = false;
		private boolean match = false;
		private Map hostMap = new HashMap();

		public HostFilter() {
			super();
		}

		public HostFilter(final int hostLimit) {
			super();
			this.hostLimit = hostLimit;
		}

		public int getHostLimit() {
			return hostLimit;
		}

		public void setHostLimit(int hostLimit) {
			this.hostLimit = hostLimit;
		}

		@Override
		public void reset() throws IOException {
			found = false;
			match = false;
		}

		@Override
		public ReturnCode filterKeyValue(Cell c) {
			KeyValue kv = KeyValueUtil.ensureKeyValue(c);
			if (kv.matchingColumn(columnFamily, columnQualifier)) {
				found = true;
				if (shouldIn(Bytes.toString(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()))) {
					match = true;
					return ReturnCode.INCLUDE;
				} else {
					return ReturnCode.NEXT_ROW;
				}
			}
			return ReturnCode.INCLUDE_AND_NEXT_COL;
		}

		public boolean hasFilterRow() {
			return true;
		}

		public boolean filterRow() {
			return found ? !match : true;
		}

		private boolean shouldIn(String value) {
			String host = getHost(value);
			if (host == null)
				return false;
			if (hostMap.containsKey(host)) {
				AtomicInteger count = (AtomicInteger) hostMap.get(host);
				if (count.getAndIncrement() <= hostLimit) {
					return true;
				} else
					return false;
			} else {
				AtomicInteger count = new AtomicInteger(1);
				hostMap.put(host, count);
				return true;
			}
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

		public byte[] toByteArray() {
			HostFilterProtos.HostFilter.Builder builder = HostFilterProtos.HostFilter.newBuilder();
			builder.setHostLimit(this.hostLimit);
			return builder.build().toByteArray();
		}

		public static HostFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
			HostFilterProtos.HostFilter proto;
			try {
				proto = HostFilterProtos.HostFilter.parseFrom(pbBytes);
			} catch (InvalidProtocolBufferException e) {
				throw new DeserializationException(e);
			}
			return new HostFilter(proto.getHostLimit());
		}

		public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
			Preconditions.checkArgument(filterArguments.size() == 1, "Expected 1 but got: %s", filterArguments.size());
			int hostLimit = ParseFilter.convertByteArrayToInt(filterArguments.get(0));
			return new HostFilter(hostLimit);
		}

		boolean areSerializedFieldsEqual(Filter o) {
			if (o == this)
				return true;
			if (!(o instanceof HostFilter))
				return false;

			HostFilter other = (HostFilter) o;
			return this.getHostLimit() == other.getHostLimit();
		}

		@Override
		public String toString() {
			return this.getClass().getSimpleName() + " " + this.hostLimit;
		}
	}

	public static void main(String[] args) throws Exception {
		// createCrawldbIdx();
		createCrawldbs();
		// regionTest();
		// showUrls();
		// getTopnUrls();
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

	public static void getTopnUrls() throws IOException {
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(T_CRAWLDBPRE + 1);
		int topn = 500;
		int hostn = 5;
		long startTime = System.currentTimeMillis();

		List<Filter> tmp = new ArrayList<Filter>();
		Filter filter1 = new PageFilter(topn);
		tmp.add(filter1);
		Filter filter2 = new HostFilter(hostn);
		tmp.add(filter2);
		FilterList filters = new FilterList(tmp);

		Scan scan = new Scan();
		scan.setCaching(topn);
		scan.setFilter(filters);

		int i = 0;
		Set set = new HashSet();
		Map hostMap = new HashMap();

		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs) {
			// System.out.println("key=" + Bytes.toString(r.getRow()));
			byte[] url = r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("url"));
			if (url != null) {
				String surl = new String(url);
				String host = getHost(surl);
				set.add(host);
				if (hostMap.containsKey(host)) {
					List list = (List) hostMap.get(host);
					list.add(Bytes.toString(r.getRow()));
				} else {
					List list = new ArrayList();
					list.add(Bytes.toString(r.getRow()));
					hostMap.put(host, list);
				}
				// System.out.println("url=" + host + "||" + surl);
			}
			if (++i >= topn)
				break;
		}
		rs.close();

		Map treeMap = new TreeMap(hostMap);
		for (Iterator iterator = treeMap.keySet().iterator(); iterator.hasNext();) {
			Object object = (Object) iterator.next();
			List list = (List) treeMap.get(object);
			if (list.size() > hostn) {
				System.err.println("host=" + object + " count=" + list.size());
				for (int j = 0; j < list.size(); j++) {
					System.out.println("region="
							+ connection.getRegionLocation(Bytes.toBytes(T_CRAWLDBPRE + 1),
									Bytes.toBytes((String) list.get(j)), false));
					// System.out.println("url=" + list.get(j));
				}
				break;
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println("getTopUrls:host=" + set.size());
		System.out.println("getTopUrls:url=" + i);
		System.out.println("getTopUrls:共耗时=" + (endTime - startTime));
		System.out.println("getTopUrls: 启动时间=" + getDate(startTime) + "结束时间=" + getDate(endTime));

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
		createTable(admin, T_CRAWLDBIDX, columnDescriptor, 1073741824l, true, getUrlSplits());
		admin.close();
	}

	public static void createCrawldbs() throws Exception {
		HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf1");
		columnDescriptor.setMaxVersions(1);

		HBaseAdmin admin = new HBaseAdmin(conf);
		// for (int i = 3; i < 101; i++) {
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
