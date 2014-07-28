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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.nutch.net.URLNormalizers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition urls by host, domain name or IP depending on the value of the
 * parameter 'partition.url.mode' which can be 'byHost', 'byDomain' or 'byIP'
 */
public class URLCountPartitioner implements Partitioner<Text, Writable> {
	private static final Logger LOG = LoggerFactory.getLogger(URLCountPartitioner.class);

	private int seed;
	private URLNormalizers normalizers;

	private Map parts = new HashMap();
	private Map newParts = new HashMap();
	private long cnt = -1;

	public void configure(JobConf job) {
		seed = job.getInt("partition.url.seed", 0);
		normalizers = new URLNormalizers(job, URLNormalizers.SCOPE_PARTITION);

		cnt = job.getLong("GENERATL_CNT", -1);
	}

	public void close() {
		LOG.info(parts.toString());
	}

	/** Hash by domain name. */
	public int getPartition(Text key, Writable value, int numReduceTasks) {
		String urlString = key.toString();
		String host = null;
		URL url = null;
		int hashCode = urlString.hashCode();
		try {
			urlString = normalizers.normalize(urlString, URLNormalizers.SCOPE_PARTITION);
			url = new URL(urlString);
			host = url.getHost();
			hashCode = host.hashCode();
		} catch (MalformedURLException e) {
			LOG.warn("Malformed URL: '" + urlString + "'");
		}

		// make hosts wind up in different partitions on different runs
		hashCode ^= seed;
		int result = (hashCode & Integer.MAX_VALUE) % numReduceTasks;

		if (cnt == -1)
			return result;

		long partCnt = cnt / numReduceTasks;
		return getPart(host, result, partCnt, numReduceTasks);
	}

	private int getPart(String host, int oriPart, long partCnt, int numReduceTasks) {
		Integer partKey = Integer.valueOf(oriPart);// 分区key
		if (parts.containsKey(partKey)) {// 存在分区
			Map hostNum = (Map) parts.get(partKey);
			AtomicLong total = (AtomicLong) hostNum.get("total");
			if (total.get() >= partCnt) {// 分区数量已够
				if (hostNum.containsKey(host)) {
					addHost(host, hostNum);
					return oriPart;
				} else {// host 不存在，换分区
					int part = getHostPart(numReduceTasks, host);
					addNewPart(host, part);
					return part;
				}
			} else {// 分区记录数不足
				addHost(host, hostNum);
				return oriPart;
			}
		} else {// 不存在分区
			addNewPart(host, partKey);
		}

		return oriPart;
	}

	// 分区新增host记录数
	private void addHost(String host, Map hostNum) {
		((AtomicLong) hostNum.get("total")).incrementAndGet();
		if (hostNum.containsKey(host)) {
			((AtomicLong) hostNum.get(host)).incrementAndGet();
		} else {
			AtomicLong num = new AtomicLong(1);
			hostNum.put(host, num);
		}
	}

	// 新增分区记录数
	private void addNewPart(String host, Integer part) {
		Map hostNum = new HashMap();
		AtomicLong num = new AtomicLong(1);
		hostNum.put(host, num);
		hostNum.put("total", new AtomicLong(1));
		parts.put(part, hostNum);
	}

	// 取host的分区, 没有取最小的
	private int getHostPart(int numReduceTasks, String host) {
		if (newParts.containsKey(host)) {
			return (Integer) newParts.get(host);
		} else {
			long min = Long.MAX_VALUE;
			int result = 0;
			for (int i = 0; i < numReduceTasks; i++) {
				Integer partKey = Integer.valueOf(i);
				if (parts.containsKey(partKey)) {
					if (((Map) parts.get(partKey)).containsKey(host)) {
						result = i;
						break;
					}
				}
				Map hostNum = (Map) parts.get(partKey);
				AtomicLong total = (AtomicLong) hostNum.get("total");
				if (total.get() < min) {
					min = total.get();
					result = i;
				}
			}

			newParts.put(host, Integer.valueOf(result));
			return result;
		}
	}

}
