package se.sics.kompics.p2p.simulator.launch;

import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class Scenario1 extends Scenario {
	private static SimulationScenario scenario = new SimulationScenario() {{
		
		StochasticProcess process1 = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1, Operations.peerJoin, uniform(Configuration.Log2Ring));
		}};

		StochasticProcess process2 = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(25, Operations.peerJoin, uniform(Configuration.Log2Ring));
		}};


		StochasticProcess process3 = new StochasticProcess() {{
			eventInterArrivalTime(constant(1000));
			raise(5, Operations.peerJoin, uniform(Configuration.Log2Ring));
			//raise(1, Operations.peerFail, uniform(Configuration.Log2Ring));
		}};
		
		StochasticProcess process4 = new StochasticProcess() {{
			eventInterArrivalTime(constant(1000));
			raise(3, Operations.subscribeEvent, uniform(Configuration.Log2Ring));
		}};
		
		StochasticProcess process5 = new StochasticProcess() {{
			eventInterArrivalTime(constant(1000));
			raise(1, Operations.publishEvent, uniform(Configuration.Log2Ring));
		}};

		process1.start();
		process2.startAfterTerminationOf(2000, process1);
		process3.startAfterTerminationOf(50000, process1);
		process4.startAfterTerminationOf(50000, process3);
		process5.startAfterTerminationOf(1000, process4);
	}};
	
//-------------------------------------------------------------------
	public Scenario1() {
		super(scenario);
	} 
}
