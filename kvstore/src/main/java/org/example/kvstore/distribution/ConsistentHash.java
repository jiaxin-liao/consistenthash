package org.example.kvstore.distribution;

import org.jgroups.Address;
import org.jgroups.View;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class ConsistentHash implements Strategy{

    private TreeSet<Integer> ring;
    private Map<Integer,Address> addresses;

    public ConsistentHash(View view){
    	ring = new TreeSet<>();
    	addresses = new HashMap<Integer, Address>();
    	List<Address> nodes = view.getMembers();
    	for(Address n : nodes) {
    		ring.add(n.hashCode());
    		addresses.put(n.hashCode(), n);
    	}
    }

    @Override
    public Address lookup(Object key){
    	Integer k = key.hashCode();
    	Integer nodePos = null;
    	if(ring.isEmpty())
    		return null;
    	if(k>ring.last())
    		nodePos = ring.first();
    	else
    		nodePos = ring.ceiling(k);
   		return addresses.get(nodePos);
    }

}
