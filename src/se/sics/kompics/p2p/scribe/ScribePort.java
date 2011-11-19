package se.sics.kompics.p2p.scribe;

import se.sics.kompics.PortType;

public final class ScribePort extends PortType {{
	negative(PublishEvent.class);
	negative(SubscribeEvent.class);
}}
