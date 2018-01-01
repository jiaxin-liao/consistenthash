package org.example.kvstore;

import org.example.kvstore.cmd.Command;
import org.example.kvstore.cmd.CommandFactory;
import org.example.kvstore.distribution.ConsistentHash;
import org.example.kvstore.distribution.Strategy;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.ReceiverAdapter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class StoreImpl<K,V> extends ReceiverAdapter implements Store<K,V>{

    private String name;
    private Strategy strategy;
    private Map<K,V> data;
    private CommandFactory<K,V> factory;
    private JChannel channel;
    private ExecutorService workers;
    
    private class CmdHandler implements Callable<Void>{
		@Override
		public Void call() throws Exception {
			// TODO Stub de la méthode généré automatiquement
			return null;
		}
	};
    
    public StoreImpl(String name) {
        this.name = name;
    }

    public void init() throws Exception{
    	data = new HashMap<>();
    	workers = Executors.newCachedThreadPool();
    	channel=new JChannel();
        channel.setReceiver(this);
        channel.connect(this.name);
//      eventLoop();
//        channel.close();
    }

    @Override
    public V get(K k) {
    	System.out.println("look up "+k);
    	Address addr = strategy.lookup(k);
    	System.out.println("addr : "+addr);
    	System.out.println("this channel : "+channel.getAddress());
    	if(addr == channel.getAddress())
    		return data.get(k);
    	return null;
    }

    @Override
    public V put(K k, V v) {
    	Address addr = strategy.lookup(k);
    	if(addr.equals(channel.getAddress())) {
    		V oldValue = data.get(k);
    		data.put(k, v);
    		return oldValue;
    	}
    	return null;
    }
    
    @Override
    public void viewAccepted(View new_view) {
    	strategy = new ConsistentHash(new_view);
    }
    
    @Override
    public void receive(Message message) {
 
    }
    
    public void Send(Address dst, Command command) {
    	Message msg = new Message(dst, null, command);
    	try {
			channel.send(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    @Override
    public String toString(){
        return "Store#"+name+"{"+data.toString()+"}";
    }

}
