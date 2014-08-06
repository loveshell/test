package org.test.test;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;

import sun.net.util.IPAddressUtil;

/**
 * Main application class for Hush - The HBase URL Shortener.
 */
public class HttpClientTest {

	public static void main(String[] args) throws Exception {
		testNginx();
	}

	public static void testNginx() throws Exception {
		String[] ips = { "10.200.5.227", "10.200.5.21", "10.200.4.232", "10.200.4.141", "10.200.4.196", "10.200.7.13",
				"10.200.4.249", "10.200.5.85", "10.200.7.22", "10.200.5.17", "10.200.7.225", "10.200.4.136",
				"10.200.6.207" };

		for (int i = 0; i < ips.length; i++) {
			httpgetThread(ips[i]);
		}
	}

	private static void httpgetThread(final String host) throws Exception {
		new Thread() {
			public void run() {
				while (true) {
					HttpClient httpClient = new DefaultHttpClient();
					httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 3000);
					httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 15000);
					HttpGet get = new HttpGet();// Get请求
					URI uri = null;
					try {
						uri = URIUtils.createURI("http", host, 80, "/", null, null);
					} catch (URISyntaxException e1) {
						e1.printStackTrace();
					}
					get.setURI(uri);
					// List<NameValuePair> qparams = new
					// ArrayList<NameValuePair>();// 设置参数
					// qparams.add(new BasicNameValuePair("cnt", "10"));

					try {
						// URI uri = URIUtils.createURI("http", host, port,
						// "/",
						// URLEncodedUtils.format(qparams, "UTF-8"), null);
						// 发送请求
						HttpResponse resp = httpClient.execute(get);
						// System.out.println(resp.getStatusLine().getStatusCode());
						// System.out.println(Arrays.asList(resp.getAllHeaders()));
						if (200 != (resp.getStatusLine().getStatusCode()))
							System.err.println(InetAddress.getByAddress(IPAddressUtil.textToNumericFormatV4(host))
									.getHostName() + " :状态码错误=" + resp.getStatusLine().getStatusCode());
						if (Arrays.asList(resp.getAllHeaders()).toString().indexOf("nginx") == -1)
							System.err.println(InetAddress.getByAddress(IPAddressUtil.textToNumericFormatV4(host))
									.getHostName() + " :非nginx服务");
						System.out.println(InetAddress.getByAddress(IPAddressUtil.textToNumericFormatV4(host))
								.getHostName() + " :is serving.");
					} catch (Exception e) {
						try {
							System.err.println(InetAddress.getByAddress(IPAddressUtil.textToNumericFormatV4(host))
									.getHostName() + " :服务不可用：" + e.getLocalizedMessage());
						} catch (UnknownHostException e1) {
							e1.printStackTrace();
						}
					} finally {
						httpClient.getConnectionManager().shutdown();
					}

					try {
						Thread.sleep(20000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
	}
}
