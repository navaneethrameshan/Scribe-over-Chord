package se.sics.kompics.p2p.peer;

import java.math.BigInteger;

public class Lookup extends PeerMessage{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4384438535607116824L;
	BigInteger lookupId;
	PeerAddress initiator;
	
	public Lookup (PeerAddress source, PeerAddress destination, PeerAddress initiator, BigInteger lookupId){
		super (source, destination);
		this.lookupId= lookupId;
		this.initiator = initiator;
	}
	
	public BigInteger getLookupId(){
		return this.lookupId;
	}
	
	public PeerAddress getInitiator(){
		return initiator;
	}
}
