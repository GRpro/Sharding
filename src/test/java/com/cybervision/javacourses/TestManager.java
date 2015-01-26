package com.cybervision.javacourses;

import com.cybervision.javacourses.client.Manager;
import com.cybervision.javacourses.master.Master;
import com.cybervision.javacourses.shard.Shard;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

import org.apache.commons.io.FileUtils;

public class TestManager {


    private static Shard[] shards = {new Shard(2001), new Shard(2002), new Shard(2003)};
    private static Master master;
    private static Manager<Integer, String> manager;

    @BeforeClass
    public static void beforeTests() {
        for (Shard shard : shards) {
            shard.start();
        }

        int ports[] = {2001, 2002, 2003};
        String[] hosts = {"localhost", "localhost", "localhost"};
        master = new Master(2000, hosts, ports);
        master.start();

        manager = new Manager<>("localhost", 2000);
    }

    @AfterClass
    public static void afterTests() {
        master.stop();
        for (Shard shard : shards) {
            shard.stop();
        }
        try {
            FileUtils.deleteDirectory(new File("storage2001"));
            FileUtils.deleteDirectory(new File("storage2002"));
            FileUtils.deleteDirectory(new File("storage2003"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSimpleCRUD() {
        String v1 = new String("object to be stored");
        Integer k1 = 1;
        assertTrue(manager.create(k1, v1));
        assertEquals(v1, manager.read(k1));
        String v2 = new String("updated");
        assertTrue(manager.update(k1, v2));
        assertFalse(manager.update(2, "not contains"));
        assertEquals(v2, manager.read(k1));
        assertTrue(manager.delete(k1));
        assertNull(manager.read(k1));
        manager.saveSession();
    }

    @Test
    public void testArrayCRUD() {
        Integer k1 = 1;

        String[] arr = new String[10000];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = new String(String.valueOf(i + 1));
        }
        assertTrue(manager.createArray(k1, arr));
        assertTrue(Arrays.equals(arr, manager.readArray(k1, String.class)));
        String[] arr1 = new String[9000];
        for (int i = 0; i < arr1.length; i++) {
            arr1[i] = new String(String.valueOf(i + 2));
        }
        assertTrue(manager.updateArray(k1, arr1));
        assertTrue(Arrays.equals(arr1, manager.readArray(k1, String.class)));
        assertTrue(manager.deleteArray(k1));
        assertNull(manager.readArray(k1, String.class));
        manager.saveSession();
    }

    @Test
    public void testCollectionCRUD() {
        Integer k1 = 1;

        Collection<String> col1 = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            col1.add(new String("str " + i));
        }
        assertTrue(manager.createCollection(k1, col1));
        Collection<String> col2 = new ArrayList<>();
        manager.readCollection(k1, col2, String.class);
        assertTrue(col1.equals(col2) && (col1.size() == col2.size()));

        Collection<String> col3 = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            col3.add(new String("updated " + i));
        }
        Collection<String> col4 = new ArrayList<>();
        assertTrue(manager.updateCollection(k1, col3));
        manager.readCollection(k1, col4, String.class);
        assertTrue(col3.equals(col4) && (col3.size() == col4.size()));

        assertTrue(manager.deleteCollection(k1));
        Collection<String> col5 = new ArrayList<>();
        try {
            manager.readCollection(k1, col5, String.class);
            assertTrue(false);
        } catch (NullPointerException e) {
            assertTrue(true);
        }
        manager.saveSession();
    }
}
