package se.sics.kompics.p2p.scribe;

import java.math.BigInteger;

import se.sics.kompics.Event;

public class SubscribeEvent extends Event{

	BigInteger topicId;
	
//-------------------------------------------------------------------	
	public SubscribeEvent(BigInteger topicId) {
		this.topicId = topicId;
	}
	
//-------------------------------------------------------------------	
	public BigInteger getTopicId() {
		return this.topicId;
	}
}
