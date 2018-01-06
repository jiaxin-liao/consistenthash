package org.example.kvstore;

import org.junit.Test;

import java.util.Random;

public class StoreTest {

    //@Test
    public void baseOperations() {
        StoreManager manager = new StoreManager();
        Store<Integer, Integer> store = manager.newStore();

        assert store.get(1) == null;
        
        store.put(42, 1);
        assert store.get(42).equals(1);

        assert store.put(42, 2).equals(1);

    }

    @Test
    	public void multipleStores(){
        int NCALLS = 1000;
        Random rand = new Random(System.nanoTime());

        StoreManager manager = new StoreManager();
        Store<Integer, Integer> store1 = manager.newStore();
        Store<Integer, Integer> store2 = manager.newStore();
        Store<Integer, Integer> store3 = manager.newStore();

        for (int i=0; i<NCALLS; i++) {
            int k = rand.nextInt();
            int v = rand.nextInt();
            System.out.println("k="+k);
            System.out.println("v="+v);
            store1.put(k, v);
            
            assert rand.nextBoolean() ? store2.get(k).equals(v) : store3.get(k).equals(v);
        }
    }

}
