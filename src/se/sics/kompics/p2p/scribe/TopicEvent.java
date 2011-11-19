package se.sics.kompics.p2p.scribe;

import java.math.BigInteger;

import se.sics.kompics.p2p.peer.PeerAddress;
import se.sics.kompics.p2p.peer.PeerMessage;

public class TopicEvent extends PeerMessage {
	private static final long serialVersionUID = 8493601671018888143L;
	private final BigInteger topicId;
	boolean fromRendezvous;

	//-------------------------------------------------------------------
	public TopicEvent(BigInteger topicId, PeerAddress source, PeerAddress destination) {
		super(source, destination);
		this.topicId = topicId;

		//Added///////////
		fromRendezvous = false;
		/////////////////
	}

	//-------------------------------------------------------------------
	public BigInteger getTopicId() {
		return this.topicId;
	}

	//-------------------------------------------------------------------
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((topicId == null) ? 0 : topicId.hashCode());
		return result;
	}

	//-------------------------------------------------------------------
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TopicEvent other = (TopicEvent) obj;
		if (topicId == null) {
			if (other.topicId != null)
				return false;
		} else if (!topicId.equals(other.topicId))
			return false;
		return true;
	}


	//------------------------------------------------------------------------
	public void setFromRendezvous(boolean status){
		fromRendezvous = status;
	}
	
	public boolean getStatus(){
		return fromRendezvous;
	}

}
