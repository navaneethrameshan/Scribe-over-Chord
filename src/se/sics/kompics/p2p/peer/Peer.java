package se.sics.kompics.p2p.peer;

import se.sics.kompics.p2p.scribe.ScribeInit;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;

import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.bootstrap.BootstrapCompleted;
import se.sics.kompics.p2p.bootstrap.BootstrapRequest;
import se.sics.kompics.p2p.bootstrap.BootstrapResponse;
import se.sics.kompics.p2p.bootstrap.P2pBootstrap;
import se.sics.kompics.p2p.bootstrap.PeerEntry;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClient;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClientInit;
import se.sics.kompics.p2p.fd.FailureDetector;
import se.sics.kompics.p2p.fd.PeerFailureSuspicion;
import se.sics.kompics.p2p.fd.StartProbingPeer;
import se.sics.kompics.p2p.fd.StopProbingPeer;
import se.sics.kompics.p2p.fd.SuspicionStatus;
import se.sics.kompics.p2p.fd.ping.PingFailureDetector;
import se.sics.kompics.p2p.fd.ping.PingFailureDetectorInit;
import se.sics.kompics.p2p.scribe.Scribe;
import se.sics.kompics.p2p.simulator.launch.Configuration;
import se.sics.kompics.p2p.simulator.snapshot.Snapshot;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

public final class Peer extends ComponentDefinition {
	public static BigInteger RING_SIZE = new BigInteger(2 + "").pow(Configuration.Log2Ring);
	public static int FINGER_SIZE = Configuration.Log2Ring;
	public static int SUCC_SIZE = Configuration.Log2Ring;
	private static int WAIT_TIME_TO_REJOIN = 15;
	private static int STABILIZING_PERIOD = 1000;

	Negative<PeerPort> msPeerPort = negative(PeerPort.class);

	Positive<Network> network = positive(Network.class);
	Positive<Timer> timer = positive(Timer.class);
//Added////////////////////////////
	Negative<ChordFingersPort> chordFingersPort = negative(ChordFingersPort.class);
////////////////////////////////////	
	
	private Component fd, bootstrap;

	private Address self;
	private PeerAddress peerSelf;

	private PeerAddress pred;
	private PeerAddress succ;
	private PeerAddress[] fingers = new PeerAddress[FINGER_SIZE];
	private PeerAddress[] succList = new PeerAddress[SUCC_SIZE];

	private int fingerIndex = 0;
	private int joinCounter = 0;
	private boolean started = false;

	private boolean bootstrapped;

	private HashMap<Address, UUID> fdRequests;
	private HashMap<Address, PeerAddress> fdPeers;

	//-------------------------------------------------------------------
	public Peer() {
		fdRequests = new HashMap<Address, UUID>();
		fdPeers = new HashMap<Address, PeerAddress>();

		for (int i = 0; i < SUCC_SIZE; i++)
			this.succList[i] = null;

		for (int i = 0; i < FINGER_SIZE; i++)
			this.fingers[i] = null;

		fd = create(PingFailureDetector.class);
		bootstrap = create(BootstrapClient.class);
		
		connect(network, fd.getNegative(Network.class));
		connect(network, bootstrap.getNegative(Network.class));
		connect(timer, fd.getNegative(Timer.class));
		connect(timer, bootstrap.getNegative(Timer.class));

		
		
		subscribe(handleInit, control);
		subscribe(handlePeriodicStabilization, timer);
		subscribe(handleJoin, msPeerPort);
		subscribe(handleFindSucc, network);
		subscribe(handleFindSuccReply, network);
		subscribe(handleWhoIsPred, network);
		subscribe(handleWhoIsPredReply, network);
		subscribe(handleNotify, network);
		subscribe(handleBootstrapResponse, bootstrap.getPositive(P2pBootstrap.class));
		subscribe(handlePeerFailureSuspicion, fd.getPositive(FailureDetector.class));
		
		//Addeed//
		subscribe(handleLookup, network);
		subscribe(handleChordFingersRequest, chordFingersPort);
	}

	//-------------------------------------------------------------------
	Handler<ChordFingersRequest> handleChordFingersRequest = new Handler<ChordFingersRequest>() {
		public void handle(ChordFingersRequest event) {
			
			trigger(new ChordFingersResponse(pred,fingers,succ), chordFingersPort );
			
		}
	};
	
	
	//-------------------------------------------------------------------
	Handler<PeerInit> handleInit = new Handler<PeerInit>() {
		public void handle(PeerInit init) {
			peerSelf = init.getMSPeerSelf();
			self = peerSelf.getPeerAddress();

			trigger(new BootstrapClientInit(self, init.getBootstrapConfiguration()), bootstrap.getControl());
			trigger(new PingFailureDetectorInit(self, init.getFdConfiguration()), fd.getControl());
			
		}
	};

	//-------------------------------------------------------------------
	Handler<JoinPeer> handleJoin = new Handler<JoinPeer>() {
		public void handle(JoinPeer event) {
			Snapshot.addPeer(peerSelf);
			BootstrapRequest request = new BootstrapRequest("chord", 1);
			trigger(request, bootstrap.getPositive(P2pBootstrap.class));
		}
	};

	//-------------------------------------------------------------------
	Handler<BootstrapResponse> handleBootstrapResponse = new Handler<BootstrapResponse>() {
		public void handle(BootstrapResponse event) {
			if (!bootstrapped) {
				bootstrapped = true;
				Set<PeerEntry> somePeers = event.getPeers();

				if (somePeers.size() == 0) {
					pred = null;
					succ = peerSelf;
					succList[0] = succ;
					Snapshot.setPred(peerSelf, pred);
					Snapshot.setSucc(peerSelf, succ);
					joinCounter = -1;
					trigger(new BootstrapCompleted("chord", peerSelf), bootstrap.getPositive(P2pBootstrap.class));
				} else {
					pred = null;
					PeerAddress existingPeer = (PeerAddress)somePeers.iterator().next().getOverlayAddress();
					trigger(new FindSucc(peerSelf, existingPeer, peerSelf, peerSelf.getPeerId(), 0), network);
					Snapshot.setPred(peerSelf, pred);
				}

				if (!started) {
					SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(STABILIZING_PERIOD, STABILIZING_PERIOD);
					spt.setTimeoutEvent(new PeriodicStabilization(spt));
					trigger(spt, timer);
					started = true;
				}
			}
		}
	};

	//-------------------------------------------------------------------
	Handler<FindSucc> handleFindSucc = new Handler<FindSucc>() {
		public void handle(FindSucc event) {
			BigInteger id = event.getID();
			PeerAddress initiator = event.getInitiator();
			int fingerIndex = event.getFingerIndex();

			if (succ != null && RingKey.belongsTo(id, peerSelf.getPeerId(), succ.getPeerId(), RingKey.IntervalBounds.OPEN_CLOSED, RING_SIZE))
				trigger(new FindSuccReply(peerSelf, initiator, succ, fingerIndex), network);
			else {
				PeerAddress nextPeer = closestPrecedingNode(id);
				trigger(new FindSucc(peerSelf, nextPeer, initiator, id, fingerIndex), network);
			}
		}
	};

	//-------------------------------------------------------------------
	Handler<FindSuccReply> handleFindSuccReply = new Handler<FindSuccReply>() {
		public void handle(FindSuccReply event) {
			PeerAddress responsible = event.getResponsible();
			int fingerIndex = event.getFingerIndex();

			if (fingerIndex == 0) {
				succ = new PeerAddress(responsible);
				succList[0] = new PeerAddress(responsible);
				Snapshot.setSucc(peerSelf, succ);
				trigger(new BootstrapCompleted("chord", peerSelf), bootstrap.getPositive(P2pBootstrap.class));
				fdRegister(succ);
				joinCounter = -1;
			}

			fingers[fingerIndex] = new PeerAddress(responsible);
			Snapshot.setFingers(peerSelf, fingers);
		}
	};
	//--------------------------------------------------------------------
	private PeerAddress performLookup (BigInteger Id){
		PeerAddress nextHop= null;
		if(Id.compareTo(peerSelf.getPeerId())==-1){
			//I am responsible. Stop Routing
			return peerSelf;
		}

		if(succ!=null && Id.compareTo(succ.getPeerId())==-1 ){
			//The successor is the responsible node. Send to successor
			return succ; 
		}
		for(int i =1; i<FINGER_SIZE; i++){
			PeerAddress previous= fingers[i-1];
			PeerAddress current = fingers[i];

			if(current!=null && Id.compareTo(current.getPeerId())<0){
				nextHop = previous;
				return nextHop;
			}

		}

		if(nextHop == null){
			//No peer found as next peer. Send to the last peer in the finger list
			return fingers[FINGER_SIZE-1];
		}
		else
			return null;

	}
	//--------------------------------------------------------------------
	Handler<Lookup> handleLookup = new Handler<Lookup>(){
		public void handle(Lookup event){
			BigInteger lookupId = event.getLookupId();
			PeerAddress nextPeer = performLookup(lookupId);
			if(nextPeer == null){
				System.out.println("[LOOKUP] Something strange happening");
			}
			System.out.println("[LOOKUP] NextPeer is: "+ nextPeer);
			if (nextPeer.getPeerId() != peerSelf.getPeerId() )
				trigger (new Lookup(peerSelf, nextPeer, event.getInitiator(), lookupId), network);
		}
	};

	//--------------------------------------------------------------------
	void startLookup(BigInteger Id){ 
		PeerAddress nextPeer = performLookup(Id);
		System.out.println("[LOOKUP] NextPeer is: "+ nextPeer);
		trigger (new Lookup(peerSelf, nextPeer, peerSelf, Id), network);
	}	


	//-------------------------------------------------------------------
	Handler<PeriodicStabilization> handlePeriodicStabilization = new Handler<PeriodicStabilization>() {
		public void handle(PeriodicStabilization event) {
		/*	//Added////
			BigInteger lookupFrom = BigInteger.valueOf(200);
			if(peerSelf.getPeerId().compareTo(lookupFrom)==-1){
				System.out.println("[LOOKUP]: Lookup started!!");
				startLookup(BigInteger.valueOf(890));
			}
			//Added///////*/
			
			if (succ == null && joinCounter != -1) { // means we haven't joined the ring yet
				if (joinCounter++ > Peer.WAIT_TIME_TO_REJOIN) { // waited enough, time to retransmit my request
					joinCounter = 0;
					bootstrapped = false;

					BootstrapRequest request = new BootstrapRequest("chord", 1);
					trigger(request, bootstrap.getPositive(P2pBootstrap.class));			
				}
			} 

			if (succ != null)
				trigger(new WhoIsPred(peerSelf, succ), network);

			// fix fingers
			if (succ == null)
				return;

			fingerIndex++;
			if (fingerIndex == FINGER_SIZE)
				fingerIndex = 1;

			BigInteger index = new BigInteger(2 + "").pow(fingerIndex);			
			BigInteger id = peerSelf.getPeerId().add(index).mod(RING_SIZE); 

			if (RingKey.belongsTo(id, peerSelf.getPeerId(), succ.getPeerId(), RingKey.IntervalBounds.OPEN_CLOSED, RING_SIZE))
				fingers[fingerIndex] = new PeerAddress(succ);
			else {
				PeerAddress nextPeer = closestPrecedingNode(id);
				trigger(new FindSucc(peerSelf, nextPeer, peerSelf, id, fingerIndex), network);
			}
		}
	};

	//-------------------------------------------------------------------
	Handler<WhoIsPred> handleWhoIsPred = new Handler<WhoIsPred>() {
		public void handle(WhoIsPred event) {
			PeerAddress requester = event.getMSPeerSource();
			trigger(new WhoIsPredReply(peerSelf, requester, pred, succList), network);
		}
	};

	//-------------------------------------------------------------------
	Handler<WhoIsPredReply> handleWhoIsPredReply = new Handler<WhoIsPredReply>() {
		public void handle(WhoIsPredReply event) {
			PeerAddress succPred = event.getPred();
			PeerAddress[] succSuccList = event.getSuccList();

			if (succ == null)
				return;

			if (succPred != null) {
				if (RingKey.belongsTo(succPred.getPeerId(), peerSelf.getPeerId(), succ.getPeerId(), RingKey.IntervalBounds.OPEN_OPEN, RING_SIZE)) {
					succ = new PeerAddress(succPred);
					fingers[0] = succ;
					succList[0] = succ;
					Snapshot.setSucc(peerSelf, succ);
					Snapshot.setFingers(peerSelf, fingers);
					fdRegister(succ);
					joinCounter = -1;
				}
			}

			for (int i = 1; i < succSuccList.length; i++) {
				if (succSuccList[i - 1] != null)
					succList[i] = new PeerAddress(succSuccList[i - 1]);
			}

			Snapshot.setSuccList(peerSelf, succList);

			if (succ != null)
				trigger(new Notify(peerSelf, succ, peerSelf), network);
		}
	};

	//-------------------------------------------------------------------
	Handler<Notify> handleNotify = new Handler<Notify>() {
		public void handle(Notify event) {
			PeerAddress newPred = event.getID();

			if (pred == null || RingKey.belongsTo(newPred.getPeerId(), pred.getPeerId(), peerSelf.getPeerId(), RingKey.IntervalBounds.OPEN_OPEN, RING_SIZE)) {
				pred = new PeerAddress(newPred);
				fdRegister(pred);
				Snapshot.setPred(peerSelf, newPred);
			}
		}
	};

	//-------------------------------------------------------------------	
	Handler<PeerFailureSuspicion> handlePeerFailureSuspicion = new Handler<PeerFailureSuspicion>() {
		public void handle(PeerFailureSuspicion event) {
			Address suspectedPeerAddress = event.getPeerAddress();

			if (event.getSuspicionStatus().equals(SuspicionStatus.SUSPECTED)) {
				if (!fdPeers.containsKey(suspectedPeerAddress) || !fdRequests.containsKey(suspectedPeerAddress))
					return;

				PeerAddress suspectedPeer = fdPeers.get(suspectedPeerAddress);
				fdUnregister(suspectedPeer);

				if (suspectedPeer.equals(pred))
					pred = null;

				if (suspectedPeer.equals(succ)) {
					int i;
					for(i = 1; i < Peer.SUCC_SIZE; i++) {
						if (succList[i] != null && !succList[i].equals(peerSelf) && !succList[i].equals(suspectedPeer)) {
							succ = succList[i];
							fingers[0] = succ;
							fdRegister(succ);
							break;
						} else
							succ = null;
					}

					joinCounter = 0;

					Snapshot.setSucc(peerSelf, succ);
					Snapshot.setFingers(peerSelf, fingers);

					for (; i > 0; i--)
						succList = leftshift(succList);
				}

				for(int i = 1; i < Peer.SUCC_SIZE; i++) {
					if (succList[i] != null && succList[i].equals(suspectedPeer))
						succList[i] = null;
				}
			}			
		}
	};

	//-------------------------------------------------------------------
	private PeerAddress closestPrecedingNode(BigInteger id) {
		for (int i = FINGER_SIZE - 1; i >= 0; i--) {
			if (fingers[i] != null && RingKey.belongsTo(fingers[i].getPeerId(), peerSelf.getPeerId(), id, RingKey.IntervalBounds.OPEN_OPEN, RING_SIZE))
				return fingers[i];
		}

		return peerSelf;
	}

	//-------------------------------------------------------------------
	private PeerAddress[] leftshift(PeerAddress[] list) {
		PeerAddress[] newList = new PeerAddress[list.length];

		for(int i = 1; i < list.length; i++)
			newList[i - 1] = list[i];

		newList[list.length - 1] = null;

		return newList;
	}
	//-------------------------------------------------------------------
	private void fdRegister(PeerAddress peer) {
		Address peerAddress = peer.getPeerAddress();
		StartProbingPeer spp = new StartProbingPeer(peerAddress, peer);
		fdRequests.put(peerAddress, spp.getRequestId());
		trigger(spp, fd.getPositive(FailureDetector.class));

		fdPeers.put(peerAddress, peer);
	}

	//-------------------------------------------------------------------	
	private void fdUnregister(PeerAddress peer) {
		if (peer == null)
			return;

		Address peerAddress = peer.getPeerAddress();
		trigger(new StopProbingPeer(peerAddress, fdRequests.get(peerAddress)), fd.getPositive(FailureDetector.class));
		fdRequests.remove(peerAddress);

		fdPeers.remove(peerAddress);
	}
}
