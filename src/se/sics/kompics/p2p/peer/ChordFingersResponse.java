package se.sics.kompics.p2p.peer;


import se.sics.kompics.Event;
import se.sics.kompics.p2p.simulator.launch.Configuration;

public class ChordFingersResponse extends Event {
	
	public static int FINGER_SIZE = Configuration.Log2Ring;
	PeerAddress pred;
	PeerAddress[] fingers;
	PeerAddress succ;

	//-------------------------------------------------------------------
		public ChordFingersResponse(PeerAddress pred, PeerAddress[] fingers, PeerAddress succ) {
			this.pred = pred;
			this.fingers = fingers;
			this.succ = succ;
		}

	//-------------------------------------------------------------------
		public PeerAddress[] getFingers() {
			return this.fingers;
		}
		
	//--------------------------------------------------------------------	
		public PeerAddress getPred() {
			return pred;
		}

	//--------------------------------------------------------------------		
		public PeerAddress getSucc() {
			return succ;
		}	
		
}
