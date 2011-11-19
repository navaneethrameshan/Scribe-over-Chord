package se.sics.kompics.p2p.scribe;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class ScribeSchedule extends Timeout {

	public ScribeSchedule(SchedulePeriodicTimeout request) {
		super(request);
	}

//-------------------------------------------------------------------
	public ScribeSchedule(ScheduleTimeout request) {
		super(request);
	}
}
