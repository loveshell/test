package org.test.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OrderUtils {

	private OrderUtils() {

	}

	/**
	 * 
	 * 对map对象 ， 按照value进行排序
	 * 
	 * @param <K>
	 *            key类型
	 * @param <V>
	 *            value 类型 ， 必须继承Comparable
	 * @param map
	 *            待排序的map
	 * @param desc
	 *            降序 true 升序false
	 * @return List<Map.Entry<K, V>> 排序后的list
	 */
	public static <K, V extends Comparable<? super V>> List<Map.Entry<K, V>> orderMapByValue(Map<K, V> map,
			final boolean desc) {
		List<Map.Entry<K, V>> list = new ArrayList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			public int compare(Entry<K, V> o1, Entry<K, V> o2) {
				if (desc) { // 降序
					if (o1.getValue() == null && o2.getValue() == null) {
						return 0;
					} else if (o2.getValue() == null) {
						return -1;
					} else if (o1.getValue() == null) {
						return 1;
					} else {
						System.out.println(o1 + " compareTo " + o2 + " " + o2.getValue().compareTo(o1.getValue()));
						return o2.getValue().compareTo(o1.getValue());
					}
				} else { // 升序
					if (o1.getValue() == null && o2.getValue() == null) {
						return 0;
					} else if (o2.getValue() == null) {
						return 1;
					} else if (o1.getValue() == null) {
						return -1;
					} else {
						return o1.getValue().compareTo(o2.getValue());
					}
				}
			}
		});
		return list;
	}

	/**
	 * 
	 * 对map对象 ， 按照key进行排序
	 * 
	 * @param <K>
	 *            key类型， 必须继承Comparable
	 * @param <V>
	 *            value 类型
	 * @param map
	 *            待排序的map
	 * @param desc
	 *            降序 true 升序false
	 * @return List<Map.Entry<K, V>> 排序后的list
	 */
	public static <K extends Comparable<? super K>, V> List<Map.Entry<K, V>> orderMapByKey(Map<K, V> map,
			final boolean desc) {
		List<Map.Entry<K, V>> list = new ArrayList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			public int compare(Entry<K, V> o1, Entry<K, V> o2) {
				if (desc) { // 降序
					if (o1.getKey() == null && o2.getKey() == null) {
						return 0;
					} else if (o2.getKey() == null) {
						return -1;
					} else if (o1.getKey() == null) {
						return 1;
					} else {
						return o2.getKey().compareTo(o1.getKey());
					}
				} else { // 升序
					if (o1.getKey() == null && o2.getKey() == null) {
						return 0;
					} else if (o2.getKey() == null) {
						return 1;
					} else if (o1.getKey() == null) {
						return -1;
					} else {
						return o1.getKey().compareTo(o2.getKey());
					}
				}
			}
		});
		return list;
	}

	/**
	 * 对一个list进行排序
	 * 
	 * 
	 * @param <E>
	 *            list中的元素必须继承Comparable
	 * @param list
	 *            待排序list
	 * @param desc
	 *            是否降序排序
	 */
	public static <E extends Comparable<? super E>> void sortList(List<E> list, final boolean desc) {
		if (list != null) {
			Collections.sort(list, new Comparator<E>() {
				public int compare(E o1, E o2) {
					if (desc) { // 降序
						if (o1 == null && o2 == null) {
							return 0;
						} else if (o2 == null) {
							return -1;
						} else if (o1 == null) {
							return 1;
						} else {
							return o2.compareTo(o1);
						}
					} else { // 升序
						if (o1 == null && o2 == null) {
							return 0;
						} else if (o2 == null) {
							return 1;
						} else if (o1 == null) {
							return -1;
						} else {
							return o1.compareTo(o2);
						}
					}
				}
			});
		}

	}

	/**
	 * 对一个set类型进行排序 ， 如果为空 ， 不操作 ， 如果存在 ， 对里面内容进行排序
	 * 
	 * @param <E>
	 *            继承比较Comparable
	 * @param set
	 *            set类型
	 * @param desc
	 *            是否降序排序
	 * @return 排序完毕list ， 如果set == null ， 返回一个空的list ， 否则 ， 返回一个按照desc赋值排序list
	 */
	public static <E extends Comparable<? super E>> List<E> sortSet(Collection<E> set, boolean desc) {
		List<E> list = new ArrayList<E>();
		if (set != null) {
			list.addAll(set);
			sortList(list, desc);
		}
		return list;
	}

	public static void main(String args[]) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		/**
		 * 
		 */
		System.out.println("2".compareTo("165"));
		map.put("1", 2);
		map.put("2", 1);
		map.put("3", null);
		map.put("4", 65);
		System.out.println(orderMapByKey(map, true));
	}

}
