package org.apache.flink.streaming.examples.wordcount;


import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Random;

public class WriteBigFile {
	public static void main(String[] args) throws IOException {
		String path = "/Users/shaolei/Desktop/work/workspace/flink-1.12.0/big_file";
		BufferedWriter writer = new BufferedWriter(new FileWriter(path));
		for (int i = 0; i < 20000000; i++) {
			writer.write(randomStr());
			writer.newLine();
		}
		System.out.println("write end");
		writer.close();
	}

	private static final String ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
	public static String randomStr(){
		Random random = new Random();
		StringBuilder sb = new StringBuilder(10);
		for (int i = 0; i < 10; i++) {
			// 从候选字符串中随机选取一个字符
			int index = random.nextInt(ALPHANUMERIC.length());
			char randomChar = ALPHANUMERIC.charAt(index);
			sb.append(randomChar);
		}
		return sb.toString();
	}
}
