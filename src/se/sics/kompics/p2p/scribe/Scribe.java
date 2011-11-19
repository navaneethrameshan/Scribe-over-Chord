package se.sics.kompics.p2p.scribe;




import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.peer.ChordFingersPort;
import se.sics.kompics.p2p.peer.ChordFingersRequest;
import se.sics.kompics.p2p.peer.ChordFingersResponse;
import se.sics.kompics.p2p.peer.PeerAddress;
import se.sics.kompics.p2p.simulator.launch.Configuration;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

//TODO: Not yet handled case where Rendezvous node fails

public final class Scribe extends ComponentDefinition {
	
	public static int FINGER_SIZE = Configuration.Log2Ring;
	
	Negative<ScribePort> scribePort = negative(ScribePort.class);
	Positive<ChordFingersPort> chordFingersPort = positive(ChordFingersPort.class);
	Positive<Network> networkPort = positive(Network.class);
	Positive<Timer> timerPort = positive(Timer.class);

	private long period;
	private PeerAddress self;

	private PeerAddress[] fingers ;
	private PeerAddress succ;
	private PeerAddress pred;
	
	HashMap<BigInteger, ArrayList<PeerAddress>> topicChildrenMap;
	HashMap<BigInteger, ArrayList<PeerAddress>> topicParentMap;
	HashMap<BigInteger, Boolean> subscribed;

	ArrayList<BigInteger> parentsTopicId;
	
//-------------------------------------------------------------------	
	public Scribe() {
	
		topicChildrenMap = new HashMap<BigInteger, ArrayList<PeerAddress>>();
		topicParentMap = new HashMap<BigInteger, ArrayList<PeerAddress>>();
		subscribed = new HashMap<BigInteger, Boolean>();
		parentsTopicId = new ArrayList<BigInteger>();
		fingers = new PeerAddress[FINGER_SIZE];
		for(int i=0;i<FINGER_SIZE;i++){
			fingers[i] = null;
		}
		
		subscribe(handleInit, control);
		subscribe(handleRequestFingers, timerPort);
		subscribe(handleFingersResponse, chordFingersPort);
		subscribe(handleScribeLookup, networkPort);
		subscribe(handleSubscribe, scribePort);
		subscribe(handlePublish, scribePort);
		subscribe(handleRecvTopicEvent, networkPort);

	}

//-------------------------------------------------------------------	
	Handler<ScribeInit> handleInit = new Handler<ScribeInit>() {
		public void handle(ScribeInit init) {
			self = init.getSelf();
			period = init.getPeriod();
			
			SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
			rst.setTimeoutEvent(new ScribeSchedule(rst));
			trigger(rst, timerPort);
		}
	};
	

//-------------------------------------------------------------------	
	Handler<ScribeSchedule> handleRequestFingers = new Handler<ScribeSchedule>() {
		public void handle(ScribeSchedule event) {
			//System.out.println("[SCRIBE] Scribe schedule event called");
			trigger(new ChordFingersRequest(), chordFingersPort);
		}
	};
//-------------------------------------------------------------------	
	Handler<ChordFingersResponse> handleFingersResponse = new Handler<ChordFingersResponse>() {
		public void handle(ChordFingersResponse event) {
			PeerAddress[] tempFingers =  event.getFingers();
			PeerAddress tempPred = event.getPred();
			PeerAddress tempSucc = event.getSucc();
			
			ArrayList<PeerAddress> changed = new ArrayList<PeerAddress>();
			
			//Get the index where the fingers have changed
			for(int i=0;i<FINGER_SIZE;i++){  //TODO: Compare both pred and succ
				if(fingers[i]!= null && tempFingers[i]!=null){
					if((fingers[i].getPeerId()).compareTo(tempFingers[i].getPeerId())!=0){
						changed.add(fingers[i]);
					
					}
				}
			}
			
			
			
			// Compare fingers with each parent
			for(int i=0;i<changed.size();i++){
				for(int j=0;j<parentsTopicId.size(); j++){
					ArrayList<PeerAddress> temp = topicParentMap.get(parentsTopicId.get(j));
					for(int k=0;k<temp.size();k++){
						if((changed.get(i).getPeerId()).compareTo(temp.get(k).getPeerId())==0){
							temp.remove(k);
							System.out.println("[SCRIBE][FINGERS UPDATE]- Updating Tree!!");
							startLookup(parentsTopicId.get(j));
						}
					}
				}
			}

			for(int i=0; i<FINGER_SIZE; i++ ){
				fingers[i]= event.getFingers()[i];
			}
			succ = event.getSucc();
			pred = event.getPred();
			
			//TODO: for children map??
			//TODO: Rendezvous node failure- only backup!!
		}
	};

//-------------------------------------------------------------------	
	public BigInteger computeHash(BigInteger topicId) { //TODO: Ensure the hash is within the idspace
		final int prime = 31;
		int result = 1;
		result = prime * result + ((topicId == null) ? 0 : topicId.hashCode());
		return BigInteger.valueOf(result);
	}

	//---------------------------------------------------------------------
	Handler<SubscribeEvent> handleSubscribe = new Handler<SubscribeEvent>() {
		public void handle(SubscribeEvent event) {
			BigInteger topicId = event.getTopicId();
			PeerAddress nextPeer;
			
			System.out.println("[SCRIBE SUBSCRIBE] Received Subscribe Event");
			BigInteger hash = computeHash(topicId);
			System.out.println("[SCRIBE SUBSCRIBE] Hash of the topic is: "+ hash);
			
			//Print Finger Table
			printFingerTable();
			
			
		//	Snapshot.publishTopicEvent(topicId);				
			nextPeer = performLookup(topicId);
			System.out.println("[SCRIBELOOKUP] SelfId is:"+ self.getPeerId());
			
			//IF I am the node with highest ID lesser than rendezvous ID. 
				//I am the Rendezvous ID
			if (self.getPeerId() == nextPeer.getPeerId()){
				System.out.println("[SCRIBE SUBSCRIBE] I am the rendezvous node. Subscribe to self");
			}
			
			
			//ELSE
				//trigger a request to that peerAddress through networkport
				// Store in a map of topic ID->parent, the destination Peer.
			else{
				startLookup(topicId);
				addToParentMap(topicId, nextPeer);
				parentsTopicId.add(topicId);
			}
				
			//Put this topic ID in the subscribed topics list
			subscribed.put(topicId, true); 
		
		}
	};
	
	//-------------------------------------------------------------------	
	Handler<PublishEvent> handlePublish = new Handler<PublishEvent>() {
		public void handle(PublishEvent event) {
			BigInteger topicId = event.getTopicId();
		//	Snapshot.publishTopicEvent(topicId);
			PeerAddress nextPeer;
			BigInteger hash = computeHash(topicId);
			System.out.println("[SCRIBE PUBLISH] The hash value is:" + hash);
			
			// trigger a request for its TmanPartners or Use the TManpartners list.
			// Find the highest peerAddress lesser than the Rendezvous ID.
			nextPeer = performLookup(topicId);
		
			//IF I am the node with highest ID lesser than rendezvous ID. 
				//I am the Rendezvous ID
			if (self.getPeerId() == nextPeer.getPeerId()){
				System.out.println("[SCRIBE PUBLISH] I am the rendezvous node. Publish to self");
			}
			
			System.out.println("[SCRIBE PUBLISH] Next Peer is : "+ nextPeer);
			//Send
			trigger(new TopicEvent(topicId, self, nextPeer), networkPort);
		
		}
	};

//-------------------------------------------------------------------	
	Handler<TopicEvent> handleRecvTopicEvent = new Handler<TopicEvent>() {
		public void handle(TopicEvent event) {
			BigInteger topicId = event.getTopicId();
			boolean receivedAtRendezvous = event.getStatus();
			BigInteger hash = computeHash(topicId);
			
			if(receivedAtRendezvous == false){
				//if received at Rendezvous Node
				if(self.compareByPeerIdTo(hash.intValue())<=0 && pred.compareByPeerIdTo(hash.intValue())>0){ 
					receivedAtRendezvous = true;
					System.out.println("[SCRIBE PUBLISH] Received the topic "+topicId+" at Rendezvous Node");
				}
			}
		
			//If rendezvous node or in the rendezvous node's path
			if(receivedAtRendezvous){

				//IF I am subscribed to the Topic
				//deliver message
				if(subscribed.containsKey(event.getTopicId())){
					System.out.println("[SCRIBE PUBLISH] Topic ID: "+ event.getTopicId() +" Recieved!! At Peer: "+ self);
				}


				// Iterate through the list of Children for that topic
				// trigger TopicEvent through network port
				if(topicChildrenMap.containsKey(topicId)){
					for(int i=0;i< topicChildrenMap.get(topicId).size(); i++){
						TopicEvent topicEvent = new TopicEvent(topicId, self, topicChildrenMap.get(topicId).get(i));
						System.out.println("[SCRIBE PUBLISH] [FROM RENDEZVOUS] Forwarding to: "+ topicChildrenMap.get(topicId).get(i));
						topicEvent.setFromRendezvous(true); //set true for future use
						trigger (topicEvent, networkPort);
					}
				}
			}
			 
				//Else it means I am forwarding to rendezvous node
			else{
				PeerAddress nextPeer= performLookup(topicId);
				System.out.println("[SCRIBE PUBLISH] Forwarding to: "+ nextPeer +". Haven't seen Rendezvous yet!");
				TopicEvent topicEvent = new TopicEvent(topicId, self, nextPeer);
				trigger (topicEvent, networkPort);
			}
			
			
			
			//Snapshot.updateRecvTopicEvent(self, topicId);
		}
	};	
	
	
//////-----------------------Added------------------------------------	

    void addToParentMap(BigInteger topicId, PeerAddress nextPeer){
    	boolean duplicate = false;
    	if (topicParentMap.containsKey(topicId)){
    		for (int i=0; i<topicParentMap.get(topicId).size();i++){
    			if(topicParentMap.get(topicId).get(i).equals(nextPeer)){
    				duplicate=true;
    				break;
    			}
    		}
    		
    		if(!duplicate)
			topicParentMap.get(topicId).add(nextPeer);
		}
		else{
			ArrayList<PeerAddress> peer = new ArrayList<PeerAddress>();
			peer.add(nextPeer);
			topicParentMap.put(topicId, peer);
		}
	}
	
//////-----------------------Added------------------------------------	

    void addToChildrenMap(BigInteger topicId, PeerAddress nextPeer){
    	boolean duplicate = false;
    	if (topicChildrenMap.containsKey(topicId)){
    		for (int i=0; i<topicChildrenMap.get(topicId).size();i++){
    			if(topicChildrenMap.get(topicId).get(i).equals(nextPeer)){
    				duplicate=true;
    				break;
    			}
    		}
    		
    		if(!duplicate)
    			topicChildrenMap.get(topicId).add(nextPeer); 
		}
		else{
			ArrayList<PeerAddress> peer = new ArrayList<PeerAddress>();
			peer.add(nextPeer);
			topicChildrenMap.put(topicId, peer);
		}
	}
	
//////-----------------------Added------------------------------------	
///**********************************************************************************************************************////////	
	
    private void printFingerTable(){
    	System.out.print("[SCRIBE] Finger table [");
    	for (int i=0;i<FINGER_SIZE;i++){
    		if(fingers[i]!= null)
    			System.out.print(fingers[i]+",");
    		else
    			System.out.print("null");
    	}
    	System.out.print("]");
    }
    
    
    //--------------------------------------------------------------------
	private PeerAddress performLookup (BigInteger Id){
		
		BigInteger hash = computeHash(Id);
		
		PeerAddress nextHop= null;
		
		if(pred!=null && self.compareByPeerIdTo(hash.intValue()) <= 0 && pred.compareByPeerIdTo(hash.intValue())>0 ){
			//I am responsible. Stop Routing
			return self;
		}

 		if(succ!=null && succ.compareByPeerIdTo(hash.intValue()) <= 0 && self.compareByPeerIdTo(hash.intValue())>0 )
 		{ 
			//The successor is the responsible node. Send to successor
			return succ; 
		}
 		
 		
		for(int i =1; i<FINGER_SIZE; i++){
			PeerAddress previous= fingers[i-1];
			PeerAddress current = fingers[i];

			if(current!=null && current.compareByPeerIdTo(hash.intValue())<=0 && previous.compareByPeerIdTo(hash.intValue())>0)
			{
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
	Handler<ScribeLookup> handleScribeLookup = new Handler<ScribeLookup>(){
		public void handle(ScribeLookup event){
			BigInteger topicId = event.getTopicId();
			PeerAddress nextPeer;
			
			//IF TopicID already exists, it means I am already a relay node.
			//Store in children map
			//return();
			if(topicChildrenMap.containsKey(event.getTopicId())){
				addToChildrenMap(event.getTopicId(), event.getMSPeerSource());
				return;
			}
			
			// Store in a map of topicID->list of children, the source node.
			addToChildrenMap(event.getTopicId(), event.getMSPeerSource());
				
			
			//ELSE  use the Fingers list.
				// Find the highest peerAddress lesser than the Rendezvous ID.
				nextPeer = performLookup(topicId);
				
				if(nextPeer == null){
					System.out.println("[SCRIBELOOKUP] Something strange happening");
				}
				
				//IF I am the node with highest ID lesser than rendezvous ID. 
						//I am the Rendezvous ID
				else if (self.getPeerId() == nextPeer.getPeerId()){
					System.out.println("[SCRIBELOOKUP] I am the rendezvous node");
				}
				
				//ELSE
					//Store in a map of topicID-> list of parents, the selected node
					// trigger a request to that peerAddress for TmanPartners through networkport
				else{
					startLookup(topicId);
					addToParentMap(event.getTopicId(), nextPeer);
				}
		}
	};
	
	//--------------------------------------------------------------------
	void startLookup(BigInteger Id){ 
		
		BigInteger hash = computeHash(Id);
		PeerAddress nextPeer = performLookup(hash);
		System.out.println("[SCRIBELOOKUP] NextPeer is: "+ nextPeer);
		trigger (new ScribeLookup(Id,self, nextPeer), networkPort);
	}	
///**********************************************************************************************************************////////	

}
