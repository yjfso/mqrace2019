package io.openmessaging;

import io.openmessaging.common.Const;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * @author yinjianfeng
 * @date 2019/8/26
 */
public class FileReadTester {

    static class Meta {
        long start;
        int length;
        public Meta(long start, int length) {
            this.start = start;
            this.length = length;
        }
    }

    private static int readNum = 10000;

    private static Meta[] metas = new Meta[readNum];

    public static void main(String[] args) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1000);
        byteBuffer.putInt(6);
        byteBuffer.slice().putInt(5);

        for (int i = 0; i < readNum; i++) {
            new Meta(
                    ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE * 2L - 100000),
                    ThreadLocalRandom.current().nextInt(100000)
            );
        }

        String path = Const.DATA_PATH + "at";
        measureTime("BufferedReader.readLine() into ArrayList", FileReadTester::bufferReaderToLinkedList, path);
        measureTime("BufferedReader.readLine() into LinkedList", FileReadTester::bufferReaderToArrayList, path);
        measureTime("Files.readAllLines()", FileReadTester::readAllLines, path);
        measureTime("Scanner.nextLine() into ArrayList", FileReadTester::scannerArrayList, path);
        measureTime("Scanner.nextLine() into LinkedList", FileReadTester::scannerLinkedList, path);
        measureTime("RandomAccessFile.readLine() into ArrayList", FileReadTester::randomAccessFileArrayList, path);
        measureTime("RandomAccessFile.readLine() into LinkedList", FileReadTester::randomAccessFileLinkedList, path);
        System.out.println("-----------------------------------------------------------");
    }

    private static void measureTime(String name, Function<String, List<String>> fn, String path) {
        System.out.println("-----------------------------------------------------------");
        System.out.println("run: " + name);
        long startTime = System.nanoTime();
        List<String> l = fn.apply(path);
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println("lines: " + l.size());
        System.out.println("estimatedTime: " + estimatedTime / 1_000_000_000.);
    }

    private static List<String> bufferReaderToLinkedList(String path) {
        return bufferReaderToList(path, new LinkedList<>());
    }

    private static List<String> bufferReaderToArrayList(String path) {
        return bufferReaderToList(path, new ArrayList<>());
    }

    private static List<String> bufferReaderToList(String path, List<String> list) {
        try {
            final BufferedReader in = new BufferedReader(
                    new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null) {
                list.add(line);
            }
            in.close();
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    private static List<String> readAllLines(String path) {
        try {
            return Files.readAllLines(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static List<String> randomAccessFileLinkedList(String path) {
        return randomAccessFile(path, new LinkedList<>());
    }

    private static List<String> randomAccessFileArrayList(String path) {
        return randomAccessFile(path, new ArrayList<>());
    }

    private static List<String> randomAccessFile(String path, List<String> list) {
        try {
            RandomAccessFile file = new RandomAccessFile(path, "r");
            for (Meta meta : metas) {
                byte[] bytes = new byte[meta.length];
            }
            String str;
            while ((str = file.readLine()) != null) {
                list.add(str);
            }
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    private static List<String> scannerLinkedList(String path) {
        return scanner(path, new LinkedList<>());
    }

    private static List<String> scannerArrayList(String path) {
        return scanner(path, new ArrayList<>());
    }

    private static List<String> scanner(String path, List<String> list) {
        try {
            Scanner scanner = new Scanner(new File(path));
            while (scanner.hasNextLine()) {
                list.add(scanner.nextLine());
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return list;
    }

}
