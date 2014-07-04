package test;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Test {
	private static Lock dbLock = new ReentrantLock();

	public static void main(String[] args) throws InterruptedException {
		testComp();
	}

	public static void testLock() throws InterruptedException {
		// System.out.println(0.0f > Float.NaN);
		// System.out.println(0.0f < Float.NaN);
		// System.out.println(0.0f == Float.NaN);

		// dbLock.lock();
		// dbLock.lock();
		// // dbLock.unlock();
		// dbLock.unlock();
		// System.out.println(dbLock.toString());
		//
		int i = 0;
		while (i < 3) {
			i++;
			System.out.println("continue do");
			dbLock.lock();
			try {

				continue;

			} catch (Exception e) {
				e.printStackTrace();

			} finally {
				dbLock.unlock();
				System.out.println("finally do ");
			}

			Thread.sleep(1000);
		}
	}

	private static void testComp() {

		System.out.println(123 % 2);
		System.out.println(123 / 2);
		System.out.println(Math.pow(3, 2));

		for (int i = 0; i < 128; i++) {
			System.out.print(Character.valueOf((char) i));
		}
		System.out.println("");
		System.out
				.println("!#$%&()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~"
						.length());
	}
}
