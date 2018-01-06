package org.example.kvstore;

import org.example.kvstore.cmd.Command;
import org.example.kvstore.cmd.CommandFactory;
import org.example.kvstore.cmd.Get;
import org.example.kvstore.cmd.Put;
import org.example.kvstore.cmd.Reply;
import org.example.kvstore.distribution.ConsistentHash;
import org.example.kvstore.distribution.Strategy;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.ReceiverAdapter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;


public class StoreImpl<K,V> extends ReceiverAdapter implements Store<K,V>{

    private String name;
    private Strategy strategy;
    private Map<K,V> data;
    private CommandFactory<K,V> factory;
    private JChannel channel;
    private ExecutorService workers;
    private CompletableFuture<V> pending;
    
    private class CmdHandler implements Callable<Void>{
    	//the address of the caller &&& the command to execute
    		Address addr;
    		Command cmd;
    		CmdHandler(Address addr, Command cmd) {
    			this.addr = addr;
    			this.cmd = cmd;
    		}
		@Override
		public Void call() throws Exception {
			V ret;
			K key = (K) cmd.getKey();
			V value = (V) cmd.getValue();
			if(cmd instanceof Reply) {
				pending.complete(value);
				return null;
			} else if(cmd instanceof Get || cmd instanceof Put)
				ret = execute(cmd);
			else
				return null;
			Reply reply = new Reply(key, ret);
			send(addr, reply);
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
	    channel.setReceiver(this); //setReceiver            NON ???????????????
	    channel.connect(this.name);
//      eventLoop();
//        channel.close();
    }
    
    @Override
    public V get(K k) {
    		return execute(new Get(k));
	}

    @Override
    public V put(K k, V v) {
    		return execute(new Put(k, v));
    }
    
    @Override
    public void viewAccepted(View new_view) {
    		System.out.println("I received a new_view is " + new_view.getMembers().size());
    		strategy = new ConsistentHash(new_view);
    }
    
    @Override
    public void receive(Message message) {
    		Address caller = message.getSrc();
    		byte[] cmdBuffer = message.getBuffer();
    		ByteArrayInputStream bis = new ByteArrayInputStream(cmdBuffer);
    		try {
			ObjectInputStream ois = new ObjectInputStream(bis);
			Command cmd = (Command) ois.readObject();
			CmdHandler cmdHdl = new CmdHandler(caller, cmd);
			workers.submit(cmdHdl);
			ois.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    }
    
    public void send(Address dst, Command command) {
    		ByteArrayOutputStream bos = new ByteArrayOutputStream();
    		try {
    			ObjectOutputStream out = new ObjectOutputStream(bos);
			out.writeObject(command);
			out.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    		Message msg = new Message(dst, null, bos.toByteArray());
    		try {
			channel.send(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    synchronized public V execute(Command cmd) {
    		K k = (K) cmd.getKey();
    		V v = (V) cmd.getValue();
    		Address addr = strategy.lookup(k);
    		System.out.println("cmd "+cmd.toString()+" lookup k="+k+" addr="+addr);
    		System.out.println("                       channel address="+channel.getAddress());
    		if(addr.equals(channel.getAddress())) {
    			if(cmd instanceof Get) {
    				System.out.println("channel2 or 3");
    				return data.get(k);
    			} else if(cmd instanceof Put) {
    				System.out.println("channel1");
    				V oldValue = data.get(k);
    	    			data.put(k, v);
    	    			return oldValue;
    			}
    		} else if(addr != null) {
    			pending = CompletableFuture.supplyAsync(() -> {
    				send(addr, cmd);
    				return null; //returned value ????????????
    			}, workers);
    			try {
				return pending.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
    		}
    		return null;
    }
    
    @Override
    public String toString(){
        return "Store#"+name+"{"+data.toString()+"}";
    }

}
