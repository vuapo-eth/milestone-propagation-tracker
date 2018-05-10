package de.mikrohash.msi;

import java.util.HashMap;

import org.zeromq.ZMQ;

public class Main {
	
	private static final int TIMEOUT_TOLERANCE = 120000;
	
	private static final String[] hosts = {
		// TODO fill with your ZMQ nodes plain host addresses (without port and protocol), e.g. "node.example.com"
	};
	
	private static final int[] lmi = new int[hosts.length];
	
    public static void main( String[] args) {
	    System.out.println("info: please add nodes to the source code in Main.java (line 12)");
    	System.out.println("connecting to " + hosts.length + " nodes ...");
    	for(int i = 0; i < hosts.length; i++) {
    		final int j = i;
    		new Thread() { public void run() { listenToMilestones(j); }; }.start();
    	}
    	while(true) {
    		synchronized (hosts) { try { hosts.wait(); } catch (InterruptedException e) { e.printStackTrace(); } }
    		printLmi();
    	}
    }
    
    private static void printLmi() {
    	final HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
    	for(int i = 0; i < lmi.length; i++)
    		map.put(lmi[i], map.containsKey(lmi[i]) ? map.get(lmi[i])+1 : 1);
    	
    	int maxLMI = 0;
    	for(Integer i : map.keySet()) maxLMI = Math.max(maxLMI, i);

    	System.out.println(System.currentTimeMillis() + " (#"+maxLMI+"): " + map.get(maxLMI) + "/" + hosts.length);
    }
    
    private static void listenToMilestones(final int hostIndex) {
    	final String address = hosts[hostIndex]+":5556";

    	ZMQ.Context context = ZMQ.context(1);
	    ZMQ.Socket requester = createSocket(context, address);
		System.out.println("connected to '"+address + "' ...");
	
	    while(true){
	    	byte[] replyBytes = requester.recv(0);
	    	
        	if(replyBytes == null) {
        		System.err.println("did not receive answer from "+address+" in "+TIMEOUT_TOLERANCE+"ms, reconnecting ...");
        		requester.close();
                requester = createSocket(context, address);
                continue;
        	}
        	
	        String reply = new String(replyBytes);
	        lmi[hostIndex] = Integer.parseInt(reply.split(" ")[2]);
    		synchronized (hosts) { hosts.notify(); }
	    }
    }

    private static ZMQ.Socket createSocket(ZMQ.Context context, String address) {
		while(true) {
			try {
		        ZMQ.Socket requester = context.socket(ZMQ.SUB);
		        requester.setReceiveTimeOut(TIMEOUT_TOLERANCE);
		        requester.connect("tcp://"+address);
		        requester.subscribe("lmi");
		        return requester;
			} catch (Throwable t) {
				System.err.println(t.toString());
				t.printStackTrace();
			}
		}
	}
}