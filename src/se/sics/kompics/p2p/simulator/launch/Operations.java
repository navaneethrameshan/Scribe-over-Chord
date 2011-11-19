package se.sics.kompics.p2p.simulator.launch;

import java.math.BigInteger;

import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation1;
import se.sics.kompics.p2p.scribe.PublishEvent;
import se.sics.kompics.p2p.scribe.SubscribeEvent;
import se.sics.kompics.p2p.simulator.PeerFail;
import se.sics.kompics.p2p.simulator.PeerJoin;

@SuppressWarnings("serial")
public class Operations {

	//-------------------------------------------------------------------
	static Operation1<PeerJoin, BigInteger> peerJoin = new Operation1<PeerJoin, BigInteger>() {
		public PeerJoin generate(BigInteger id) {
			return new PeerJoin(id);
		}
	};

	//-------------------------------------------------------------------
	static Operation1<PeerFail, BigInteger> peerFail = new Operation1<PeerFail, BigInteger>() {
		public PeerFail generate(BigInteger id) {
			return new PeerFail(id);
		}
	};

	//-------------------------------------------------------------------
	static Operation1<SubscribeEvent, BigInteger> subscribeEvent = new Operation1<SubscribeEvent, BigInteger>() {
		public SubscribeEvent generate(BigInteger id) {
			return new SubscribeEvent(BigInteger.valueOf(1000)); // TODO: Change later
		}
	};
	//-------------------------------------------------------------------
	static Operation1<PublishEvent, BigInteger> publishEvent = new Operation1<PublishEvent, BigInteger>() {
		public PublishEvent generate(BigInteger id) {
			return new PublishEvent(BigInteger.valueOf(1000)); // TODO: Change later
		}
	};

}
