/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.test.hadoop;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.net.URLNormalizers;

/**
 * Partition urls by host, domain name or IP depending on the value of the
 * parameter 'partition.url.mode' which can be 'byHost', 'byDomain' or 'byIP'
 */
public class URLCountPartitioner implements Partitioner<Text, Writable> {
	private int seed;
	private URLNormalizers normalizers;

	private Map partHostNums = new HashMap();
	private Map hostParts2 = new HashMap();
	private Map hostParts = new HashMap();
	private long topn = 100000;
	private int hostn = -1;
	private String cntStr;
	private boolean initPart = false;

	public void close() {
	}

	public void configure(JobConf job) {
		seed = job.getInt("partition.url.seed", 0);
		normalizers = new URLNormalizers(job, URLNormalizers.SCOPE_PARTITION);

		topn = job.getLong(Generator.GENERATOR_TOP_N, 100000);
		hostn = job.getInt(Generator.GENERATOR_MAX_COUNT, -1);
		// cntStr = job.get(GeneratorHbase.GENERATL_CNT);// 不能获得
		// int reduceNum = job.getInt(GeneratorHbase.GENERATL_REDUCENUM, 1);
		// initPart(reduceNum);
	}

	/** Hash by domain name. */
	public int getPartition(Text key, Writable value, int numReduceTasks) {
		// if (!initPart) {
		// initPart(numReduceTasks);
		// GeneratorHbase.LOG.info("*************************************");
		// }

		// if (cntStr == null || cntStr.length() == 0)
		return getHostPartition(key, numReduceTasks);

		// return getPartition(key, numReduceTasks);
	}

	public static void main(String[] args) {
		URLCountPartitioner urlCountPartitioner = new URLCountPartitioner();
		JobConf job = new JobConf();

		// job.set("generate.cnt",
		// "partTotal:20000,10.200.4.136:2319,10.200.6.207:20001,10.200.4.141:9408,10.200.5.85:18004,10.200.7.13:10266,10.200.7.225:20002");
		job.set("generate.cnt",
				"partTotal:20000,10.200.4.136:953,10.200.6.207:20001,10.200.4.141:8578,10.200.7.22:20001,10.200.7.13:20001,10.200.7.225:10466");
		job.setInt("generate.reduceNum", 4);

		urlCountPartitioner.configure(job);
	}

	private void initPart(int reduceNum) {
		if (cntStr != null && cntStr.length() != 0) {
			initPart = true;

			String[] tmp = cntStr.split(",");
			long partLimit = 0;
			List<String[]> hostCntList = new ArrayList<String[]>();
			Map hostNumValues = new HashMap();
			for (int i = 0; i < tmp.length; i++) {
				String[] hostCnt = tmp[i].split(":");
				if ("partTotal".equals(hostCnt[0]))
					partLimit = Long.parseLong(hostCnt[1]);
				else {
					hostCntList.add(hostCnt);
					hostNumValues.put(hostCnt[0], Long.parseLong(hostCnt[1]));
				}
			}

			Collections.sort(hostCntList, new Comparator<String[]>() {
				public int compare(String[] o1, String[] o2) {
					return Long.valueOf(o2[1]).compareTo(Long.valueOf(o1[1]));
				}
			});// 大到小排序

			// List<String[]> bigHosts = hostCntList;
			// if (hostCntList.size() > reduceNum) {
			// bigHosts = hostCntList.subList(0, reduceNum);
			// hostCntList = hostCntList.subList(reduceNum, hostCntList.size());
			// }
			for (int i = 0; i < reduceNum; i++) {// 优先填满
				if (hostCntList.size() > i) {
					String[] current = hostCntList.get(i);
					long curNum = Long.parseLong(current[1]);
					String host = current[0];
					Integer part = Integer.valueOf(i);
					hostParts.put(host, part);

					approachLimit(partLimit, curNum, hostCntList, part);
				}
			}

			testPrint(hostNumValues);
		}
	}

	private void testPrint(Map hostNumValue) {
		Map partHosts = new HashMap();
		for (Iterator iterator = hostParts.keySet().iterator(); iterator.hasNext();) {
			String host = (String) iterator.next();
			Integer part = (Integer) hostParts.get(host);

			if (partHosts.containsKey(part)) {
				partHosts.put(part, ((Long) partHosts.get(part)) + ((Long) hostNumValue.get(host)));
			} else {
				partHosts.put(part, (Long) hostNumValue.get(host));
			}
		}

		GeneratorHbase2.LOG.info(hostNumValue.toString());
		GeneratorHbase2.LOG.info(hostParts.toString());
		GeneratorHbase2.LOG.info(partHosts.toString());
	}

	// partLimit > total
	private void approachLimit(long partLimit, long total, List<String[]> data, Integer part) {
		if (data == null || data.isEmpty())
			return;

		long distance = partLimit - total;
		if (distance <= 0)
			return;

		// 算出差值
		for (int i = 0; i < data.size(); i++) {
			String[] current = data.get(i);
			long curNum = Long.parseLong(current[1]);
			String host = current[0];

			if (curNum == distance) {// 加起来刚好为均值
				hostParts.put(host, part);
				data.remove(i);// 移除
				return;
			} else if (curNum < distance) {// 取小于差值
				hostParts.put(host, part);
				data.remove(i);// 移除
				approachLimit(partLimit, total + curNum, data, part);// 递归
				return;
			}
		}

	}

	private int getPartition(Text key, int numReduceTasks) {
		String urlString = key.toString();
		String host = null;
		try {
			urlString = normalizers.normalize(urlString, URLNormalizers.SCOPE_PARTITION);
			URL url = new URL(urlString);
			host = url.getHost();
		} catch (MalformedURLException e) {
			GeneratorHbase2.LOG.warn("Malformed URL: '" + urlString + "'");
		}

		if (host == null)
			return getHostPartition(key, numReduceTasks);

		return ((Integer) hostParts.get(host)).intValue();
	}

	/** Hash by domain name. */
	private int getHashPartition(Text key, Writable value, int numReduceTasks) {
		String urlString = key.toString();
		URL url = null;
		int hashCode = urlString.hashCode();
		try {
			urlString = normalizers.normalize(urlString, URLNormalizers.SCOPE_PARTITION);
			url = new URL(urlString);
			hashCode = url.getHost().hashCode();
		} catch (MalformedURLException e) {
			GeneratorHbase2.LOG.warn("Malformed URL: '" + urlString + "'");
		}
		// make hosts wind up in different partitions on different runs
		hashCode ^= seed;

		return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
	}

	private int getHostPartition(Text key, int numReduceTasks) {
		String urlString = key.toString();
		String host = null;
		int hashCode = urlString.hashCode();
		try {
			urlString = normalizers.normalize(urlString, URLNormalizers.SCOPE_PARTITION);
			URL url = new URL(urlString);
			host = url.getHost();
			hashCode = host.hashCode();
		} catch (MalformedURLException e) {
			GeneratorHbase2.LOG.warn("Malformed URL: '" + urlString + "'");
		}

		// make hosts wind up in different partitions on different runs
		hashCode ^= seed;
		int oriPart = (hashCode & Integer.MAX_VALUE) % numReduceTasks;

		long partLimit = numReduceTasks;// 不精确
		if (hostn != -1) {
			partLimit = hostn;
		}

		return getPart(host, oriPart, partLimit, numReduceTasks);
	}

	private int getPart(String host, int oriPart, long partLimit, int numReduceTasks) {
		Integer partKey = Integer.valueOf(oriPart);// 分区key
		if (partHostNums.containsKey(partKey)) {// 存在分区
			Map hostNums = (Map) partHostNums.get(partKey);
			AtomicLong total = (AtomicLong) hostNums.get("total");
			if (total.get() >= partLimit) {// 分区数量已够
				if (hostNums.containsKey(host)) {
					addHost(host, hostNums);
					return oriPart;
				} else {// host 不存在，换分区
					int part = getHostPart(numReduceTasks, host);
					addNewPart(host, part);
					return part;
				}
			} else {// 分区记录数不足
				addHost(host, hostNums);
				return oriPart;
			}
		} else {// 不存在分区
			addNewPart(host, partKey);
		}

		return oriPart;
	}

	// 分区新增host记录数
	private void addHost(String host, Map hostNums) {
		((AtomicLong) hostNums.get("total")).incrementAndGet();
		if (hostNums.containsKey(host)) {
			((AtomicLong) hostNums.get(host)).incrementAndGet();
		} else {
			AtomicLong num = new AtomicLong(1);
			hostNums.put(host, num);
		}
	}

	// 新增分区记录数
	private void addNewPart(String host, Integer part) {
		Map hostNums = new HashMap();
		AtomicLong num = new AtomicLong(1);
		hostNums.put(host, num);
		hostNums.put("total", new AtomicLong(1));
		partHostNums.put(part, hostNums);
	}

	// 取host的分区, 没有取最小的
	private int getHostPart(int numReduceTasks, String host) {
		if (hostParts2.containsKey(host)) {
			return (Integer) hostParts2.get(host);
		} else {
			long min = Long.MAX_VALUE;
			int part = -1;
			for (int i = 0; i < numReduceTasks; i++) {// 是否有分区含这个host
				Integer partKey = Integer.valueOf(i);
				if (partHostNums.containsKey(partKey)) {
					if (((Map) partHostNums.get(partKey)).containsKey(host)) {
						part = i;
						break;
					}
				}
			}
			if (part == -1) {// not found
				for (int i = 0; i < numReduceTasks; i++) {
					Integer partKey = Integer.valueOf(i);
					if (partHostNums.containsKey(partKey)) {
						Map hostNum = (Map) partHostNums.get(partKey);
						AtomicLong total = (AtomicLong) hostNum.get("total");
						if (total.get() < min) {
							min = total.get();
							part = i;
						}
					} else {
						// 分区未含有数据
						part = i;
						break;
					}
				}
			}

			hostParts2.put(host, Integer.valueOf(part));
			return part;
		}
	}
}
