package se.sics.kompics.p2p.simulator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import se.sics.kompics.ChannelFilter;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;
import se.sics.kompics.p2p.fd.ping.PingFailureDetectorConfiguration;
import se.sics.kompics.p2p.peer.ChordFingersPort;
import se.sics.kompics.p2p.peer.JoinPeer;
import se.sics.kompics.p2p.peer.Peer;
import se.sics.kompics.p2p.peer.PeerAddress;
import se.sics.kompics.p2p.peer.PeerConfiguration;
import se.sics.kompics.p2p.peer.PeerInit;
import se.sics.kompics.p2p.peer.PeerPort;
import se.sics.kompics.p2p.scribe.PublishEvent;
import se.sics.kompics.p2p.scribe.Scribe;
import se.sics.kompics.p2p.scribe.ScribeInit;
import se.sics.kompics.p2p.scribe.ScribePort;
import se.sics.kompics.p2p.scribe.SubscribeEvent;
import se.sics.kompics.p2p.simulator.launch.Configuration;
import se.sics.kompics.p2p.simulator.snapshot.Snapshot;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

public final class Simulator extends ComponentDefinition {

	Positive<SimulatorPort> simulator = positive(SimulatorPort.class);
	Positive<Network> network = positive(Network.class);
	Positive<Timer> timer = positive(Timer.class);

	private int peerIdSequence;

	private Address peer0Address;
	private BigInteger idSpaceSize;
	private ConsistentHashtable<BigInteger> view;
	private final HashMap<BigInteger, Component> peers;
	private final HashMap<BigInteger, PeerAddress> peersAddress;
	//Added/////////
	private final HashMap<BigInteger, Component> scribeList;
	private final ArrayList<BigInteger> peerIdList;
	//////////////////
	
	private BootstrapConfiguration bootstrapConfiguration;
	private PeerConfiguration peerConfiguration;	
	private PingFailureDetectorConfiguration fdConfiguration;

//-------------------------------------------------------------------	
	public Simulator() {
		peers = new HashMap<BigInteger, Component>();
		peersAddress = new HashMap<BigInteger, PeerAddress>();
		view = new ConsistentHashtable<BigInteger>();
		scribeList = new HashMap<BigInteger, Component>();
		peerIdList = new ArrayList<BigInteger>();
		subscribe(handleInit, control);
		
		subscribe(handleGenerateReport, timer);
		
		subscribe(handlePeerJoin, simulator);
		subscribe(handlePeerFail, simulator);
		subscribe(handleSubscribeEvent, simulator);
		subscribe(handlePublishEvent, simulator);
	}

//-------------------------------------------------------------------	
	Handler<SimulatorInit> handleInit = new Handler<SimulatorInit>() {
		public void handle(SimulatorInit init) {
			peers.clear();
			//////////////////////////
			scribeList.clear();
			/////////////////////////
			peerIdSequence = 0;

			peer0Address = init.getPeer0Address();
			bootstrapConfiguration = init.getBootstrapConfiguration();
			fdConfiguration = init.getFdConfiguration();
			peerConfiguration = init.getMSConfiguration();
			
			idSpaceSize = new BigInteger(2 + "").pow(Configuration.Log2Ring);

			
			int snapshotPeriod = peerConfiguration.getSnapshotPeriod();			
			SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(snapshotPeriod, snapshotPeriod);
			spt.setTimeoutEvent(new GenerateReport(spt));
			trigger(spt, timer);
		}
	};

//-------------------------------------------------------------------	
	Handler<PeerJoin> handlePeerJoin = new Handler<PeerJoin>() {
		public void handle(PeerJoin event) {
			BigInteger id = event.getPeerId();
			
			// join with the next id if this id is taken
			BigInteger successor = view.getNode(id);
			while (successor != null && successor.equals(id)) {
				id = id.add(BigInteger.ONE).mod(idSpaceSize);
				successor = view.getNode(id);
			}

			Component newPeer = createAndStartNewPeer(id);
			view.addNode(id);

			trigger(new JoinPeer(id), newPeer.getPositive(PeerPort.class));
		}
	};

//-------------------------------------------------------------------	
	Handler<PeerFail> handlePeerFail = new Handler<PeerFail>() {
		public void handle(PeerFail event) {
			BigInteger id = view.getNode(event.getPeerId());

			if (view.size() == 0) {
				System.err.println("Empty network");
				return;
			}

			view.removeNode(id);
			//Added/////////
			peerIdList.remove(id);
			////////////////
			stopAndDestroyPeer(id);
		}
	};
	//Added-------------------------------------------------------------------
	Handler<SubscribeEvent> handleSubscribeEvent = new Handler<SubscribeEvent>() {
		public void handle(SubscribeEvent event) {
			BigInteger topicId = event.getTopicId();
			//System.out.println("[SIMULATOR] [SUBSCRIBE] Received SUBSCRIBE Event");
		    int index= new Random(System.nanoTime()).nextInt(peerIdList.size());
		    Component randomScribeComponent =scribeList.get(peerIdList.get(index));
		    //System.out.println("[SIMULATOR] [SUBSCRIBE] Chosen Peer: "+ peerIdList.get(index));
		    trigger(new SubscribeEvent(topicId),randomScribeComponent.getPositive(ScribePort.class));
		}
	};
	
	//Added-------------------------------------------------------------------	
	Handler<PublishEvent> handlePublishEvent = new Handler<PublishEvent>() {
		public void handle(PublishEvent event) {
			BigInteger topicId = event.getTopicId();
			//System.out.println("[SIMULATOR] [PUBLISH] Received PUBLISH Event");
		    int index= new Random(System.nanoTime()).nextInt(peerIdList.size());
		    Component randomScribeComponent =scribeList.get(peerIdList.get(index));
		    //System.out.println("[SIMULATOR] [PUBLISH] Chosen Peer: "+ peerIdList.get(index));
		    trigger(new PublishEvent(topicId),randomScribeComponent.getPositive(ScribePort.class));
		}
	};
	
	
//-------------------------------------------------------------------	
	Handler<GenerateReport> handleGenerateReport = new Handler<GenerateReport>() {
		public void handle(GenerateReport event) {
			//Snapshot.report();
		}
	};

//-------------------------------------------------------------------	
	private final Component createAndStartNewPeer(BigInteger id) {
		Component peer = create(Peer.class);
		//Added//
		Component scribe = create (Scribe.class);
		////////

		int peerId = ++peerIdSequence;
		Address peerAddress = new Address(peer0Address.getIp(), peer0Address.getPort(), peerId);

		PeerAddress msPeerAddress = new PeerAddress(peerAddress, id);
		
		connect(network, peer.getNegative(Network.class), new MessageDestinationFilter(peerAddress));
		connect(timer, peer.getNegative(Timer.class));

		//Added///
		connect(network, scribe.getNegative(Network.class), new MessageDestinationFilter(peerAddress));
		connect(timer, scribe.getNegative(Timer.class));
		connect(peer.getPositive(ChordFingersPort.class), scribe.getNegative(ChordFingersPort.class));
		///////////
		
		trigger(new PeerInit(msPeerAddress, peerConfiguration, bootstrapConfiguration, fdConfiguration), peer.getControl());

		//Added///
		trigger(new ScribeInit(msPeerAddress,1000), scribe.getControl());
		/////////
		
		trigger(new Start(), peer.getControl());
		peers.put(id, peer);
		
		////Added////////
		scribeList.put(id, scribe);
		////Added////////
		peersAddress.put(id, msPeerAddress);
		peerIdList.add(id);
		
		return peer;
	}

//-------------------------------------------------------------------	
	private final void stopAndDestroyPeer(BigInteger id) {
		Component peer = peers.get(id);

		trigger(new Stop(), peer.getControl());

		disconnect(network, peer.getNegative(Network.class));
		disconnect(timer, peer.getNegative(Timer.class));

		Snapshot.removePeer(peersAddress.get(id));

		peers.remove(id);
		peersAddress.remove(id);

		destroy(peer);
	}

//-------------------------------------------------------------------	
	private final static class MessageDestinationFilter extends ChannelFilter<Message, Address> {
		public MessageDestinationFilter(Address address) {
			super(Message.class, address, true);
		}

		public Address getValue(Message event) {
			return event.getDestination();
		}
	}
}

