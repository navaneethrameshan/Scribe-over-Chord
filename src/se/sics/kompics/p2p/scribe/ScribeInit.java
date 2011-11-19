package se.sics.kompics.p2p.scribe;

import java.math.BigInteger;
import java.util.ArrayList;

import se.sics.kompics.Init;
import se.sics.kompics.p2p.peer.PeerAddress;

public final class ScribeInit extends Init {

	private final PeerAddress peerSelf;
	//private final ArrayList<BigInteger> topics;
	private final long period;

//-------------------------------------------------------------------
	public ScribeInit(PeerAddress peerSelf, long period) {
		super();
		this.peerSelf = peerSelf;
	//	this.topics = topics;
		this.period = period;
	}

//-------------------------------------------------------------------
	public PeerAddress getSelf() {
		return this.peerSelf;
	}
	
//--------------------------------------------------------------------
	public long getPeriod(){
		return this.period;
	}

}