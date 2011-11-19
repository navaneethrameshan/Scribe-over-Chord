package se.sics.kompics.p2p.simulator;

import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;
import se.sics.kompics.p2p.scribe.PublishEvent;
import se.sics.kompics.p2p.scribe.SubscribeEvent;

public class SimulatorPort extends PortType {{
	positive(PeerJoin.class);
	positive(PeerFail.class);	
	negative(TerminateExperiment.class);
	positive(SubscribeEvent.class);
	positive(PublishEvent.class);
}}
