package se.sics.kompics.p2p.peer;

import se.sics.kompics.PortType;

public class ChordFingersPort extends PortType {{
	negative(ChordFingersRequest.class);
	positive(ChordFingersResponse.class);

}}
