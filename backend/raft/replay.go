package raft

// Scenario is a savable/replayable recording: initial configuration plus
// every external action (proposals, partitions, heals, link tuning).
// Replay rebuilds a fresh engine from the same seed and fires each
// recorded action at its recorded tick, yielding identical elections,
// commits and rejections.
type Scenario struct {
	Name    string   `json:"name"`
	Nodes   int      `json:"nodes"`
	Seed    int      `json:"seed"`
	Actions []Action `json:"actions"`
}

func (e *Engine) Scenario(name string) Scenario {
	return Scenario{
		Name:    name,
		Nodes:   e.Size,
		Seed:    e.Seed,
		Actions: e.Actions(),
	}
}

// ReplayEngine builds an engine primed to reproduce s. Call Tick() up to
// untilTick; scheduled actions fire at their original ticks.
func ReplayEngine(s Scenario) *Engine {
	nodes := s.Nodes
	if nodes < 1 {
		nodes = 5
	}
	e := NewEngine(nodes, s.Seed)
	for _, a := range s.Actions {
		e.ScheduleReplayAction(a)
	}
	return e
}

// Replay runs a scenario to completion (maxTick or the last action tick
// plus a settling window) and returns the resulting snapshot/events.
func Replay(s Scenario, maxTick int) (*Engine, []Event) {
	e := ReplayEngine(s)
	end := maxTick
	if end <= 0 {
		end = 0
		for _, a := range s.Actions {
			if a.Tick > end {
				end = a.Tick
			}
		}
		end += 40
	}
	for e.tick < end {
		e.Tick()
	}
	return e, e.Events()
}
