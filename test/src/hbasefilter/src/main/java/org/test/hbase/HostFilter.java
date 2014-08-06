package org.test.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class HostFilter extends FilterBase {
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
	public void reset() {
		found = false;
		match = false;
	}

	public ReturnCode filterKeyValue(KeyValue kv) {
		if (kv.matchingColumn(columnFamily, columnQualifier)) {
			found = true;
			if (shouldIn(Bytes.toString(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength()))) {
				match = true;
				return ReturnCode.INCLUDE;
			} else {
				return ReturnCode.NEXT_ROW;
			}
		}
		return ReturnCode.INCLUDE_AND_NEXT_COL;
	}

	// @Override
	// public void reset() throws IOException {
	// found = false;
	// match = false;
	// }

	// @Override
	// public ReturnCode filterKeyValue(Cell c) {
	// KeyValue kv = KeyValueUtil.ensureKeyValue(c);
	// if (kv.matchingColumn(columnFamily, columnQualifier)) {
	// found = true;
	// if (shouldIn(Bytes.toString(kv.getValueArray(), kv.getValueOffset(),
	// kv.getValueLength()))) {
	// match = true;
	// return ReturnCode.INCLUDE;
	// } else {
	// return ReturnCode.NEXT_ROW;
	// }
	// }
	// return ReturnCode.INCLUDE_AND_NEXT_COL;
	// }

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
			if (count.incrementAndGet() <= hostLimit) {
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

	// public static HostFilter parseFrom(final byte[] pbBytes) throws
	// DeserializationException {
	// HostFilterProtos.HostFilter proto;
	// try {
	// proto = HostFilterProtos.HostFilter.parseFrom(pbBytes);
	// } catch (InvalidProtocolBufferException e) {
	// throw new DeserializationException(e);
	// }
	// return new HostFilter(proto.getHostLimit());
	// }

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

	// ºÊ»›µÕ∞Ê±æ
	public void readFields(final DataInput in) throws IOException {
		hostLimit = in.readInt();
	}

	public void write(final DataOutput out) throws IOException {
		out.writeInt(hostLimit);
	}
}
