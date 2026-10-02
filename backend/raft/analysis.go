package raft

import "fmt"

type InterventionRef struct {
	ExperimentID string `json:"experimentId"`
	DecisionID   string `json:"decisionId"`
	ActionIndex  int    `json:"actionIndex"`
	At           int64  `json:"at"`
	Label        string `json:"label"`
}

type DivergenceAnalysis struct {
	ID             string          `json:"id"`
	ExperimentAID  string          `json:"experimentAId"`
	ExperimentBID  string          `json:"experimentBId"`
	CreatedAt      string          `json:"createdAt"`
	PausePoint     *Selector       `json:"pausePoint"`
	Candidate      InterventionRef `json:"candidate"`
	ControlID      string          `json:"controlId"`
	Classification string          `json:"classification"`
	Sufficient     bool            `json:"sufficient"`
	Complete       bool            `json:"complete"`
	Explanation    string          `json:"explanation"`
	FirstDecisionA Decision        `json:"firstDecisionA"`
	FirstDecisionB Decision        `json:"firstDecisionB"`
	MessageA       *Message        `json:"messageA,omitempty"`
	MessageB       *Message        `json:"messageB,omitempty"`
	ControlState   *State          `json:"controlState,omitempty"`
	TreatedState   *State          `json:"treatedState,omitempty"`
	OppositeState  *State          `json:"oppositeState,omitempty"`
	ControlDoc     ExperimentDoc   `json:"controlDoc"`
	TreatedDoc     ExperimentDoc   `json:"treatedDoc"`
}

func LoadExperimentAtDecision(doc ExperimentDoc, decisionID string) (*Simulator, error) {
	s, _ := RunScenario(doc.Base, true)
	for _, decision := range doc.Decisions {
		if decision.At > s.Now() {
			s.Advance(decision.At - s.Now())
		}
		for _, action := range decision.Actions {
			if err := applyLoggedAction(s, action); err != nil {
				return nil, err
			}
		}
		if decision.ID == decisionID {
			return s, nil
		}
	}
	if decisionID == "" {
		return s, nil
	}
	return nil, fmt.Errorf("decision %s not found", decisionID)
}

func LoadExperimentBeforeDecision(doc ExperimentDoc, decisionID string) (*Simulator, error) {
	s, _ := RunScenario(doc.Base, true)
	for _, decision := range doc.Decisions {
		if decision.At > s.Now() {
			s.Advance(decision.At - s.Now())
		}
		if decision.ID == decisionID {
			return s, nil
		}
		for _, action := range decision.Actions {
			if err := applyLoggedAction(s, action); err != nil {
				return nil, err
			}
		}
	}
	return nil, fmt.Errorf("decision %s not found", decisionID)
}

func LoadExperimentControlAtDecision(doc ExperimentDoc, decisionID string, skipActionIndex int) (*Simulator, error) {
	s, _ := RunScenario(doc.Base, true)
	for _, decision := range doc.Decisions {
		if decision.At > s.Now() {
			s.Advance(decision.At - s.Now())
		}
		if decision.ID == decisionID {
			for index, action := range decision.Actions {
				if index == skipActionIndex {
					continue
				}
				if err := applyLoggedAction(s, action); err != nil {
					return nil, err
				}
			}
			return s, nil
		}
		for _, action := range decision.Actions {
			if err := applyLoggedAction(s, action); err != nil {
				return nil, err
			}
		}
	}
	return nil, fmt.Errorf("decision %s not found", decisionID)
}

func ExperimentWithoutAction(doc ExperimentDoc, ref InterventionRef) (ExperimentDoc, Action, error) {
	out := doc
	out.ID = ref.ExperimentID + "-control-" + ref.DecisionID + "-" + fmt.Sprint(ref.ActionIndex)
	out.Name = "control"
	out.ReadOnly = true
	out.Decisions = append([]Decision(nil), doc.Decisions...)
	var removed Action
	found := false
	for i := range out.Decisions {
		if out.Decisions[i].ID != ref.DecisionID {
			continue
		}
		if ref.ActionIndex < 0 || ref.ActionIndex >= len(out.Decisions[i].Actions) {
			return ExperimentDoc{}, Action{}, fmt.Errorf("intervention action index out of range")
		}
		removed = out.Decisions[i].Actions[ref.ActionIndex]
		actions := append([]Action(nil), out.Decisions[i].Actions[:ref.ActionIndex]...)
		actions = append(actions, out.Decisions[i].Actions[ref.ActionIndex+1:]...)
		out.Decisions[i].Actions = actions
		found = true
		break
	}
	if !found {
		return ExperimentDoc{}, Action{}, fmt.Errorf("intervention decision not found")
	}
	return out, removed, nil
}

func applyLoggedAction(s *Simulator, action Action) error {
	switch action.Op {
	case ActionPause:
		s.ArmPause(action.selector())
	case ActionDeliverFirst:
		if err := s.DeliverFirst(action.selector()); err != nil {
			return err
		}
	case ActionPostpone:
		if err := s.Postpone(action.selector(), action.Delay); err != nil {
			return err
		}
	case ActionAdvance:
		s.Advance(action.Delay)
		s.RecordAdvance(action.At, action.At+action.Delay)
	case "delay":
		if err := s.SetDelay(action.From, action.To, action.Delay); err != nil {
			return err
		}
	case "link":
		up := action.Up == nil || *action.Up
		s.SetLink(action.From, action.To, up)
	case "partition":
		s.Partition(action.Groups)
	case "heal":
		s.HealAll()
	case "client":
		if err := s.Client(action.Node, action.Data); err != nil {
			return err
		}
	}
	return nil
}

func FirstDifferentDecision(a, b ExperimentDoc) (Decision, Decision, int) {
	for i := 0; ; i++ {
		if i >= len(a.Decisions) || i >= len(b.Decisions) {
			return Decision{}, Decision{}, -1
		}
		if !sameDecision(a.Decisions[i], b.Decisions[i]) {
			return a.Decisions[i], b.Decisions[i], i
		}
	}
}

func sameDecision(a, b Decision) bool {
	if a.At != b.At || a.Kind != b.Kind || len(a.Actions) != len(b.Actions) {
		return false
	}
	for i := range a.Actions {
		if fmt.Sprint(a.Actions[i]) != fmt.Sprint(b.Actions[i]) {
			return false
		}
	}
	return true
}

func ActionMessage(action Action) *Message {
	if action.MsgType == "" {
		return nil
	}
	return &Message{Type: MessageType(action.MsgType), From: action.From, To: action.To, Term: action.Term, Data: action.Data, SendAt: action.At, DeliverAt: action.At}
}

func FindQueuedMessage(s *Simulator, sel Selector) *Message {
	for _, message := range s.PendingMessages() {
		if sel.Matches(message) {
			copied := message
			return &copied
		}
	}
	return nil
}

func NodeRecordsDiffer(a, b State) bool {
	if len(a.Nodes) != len(b.Nodes) {
		return true
	}
	for i := range a.Nodes {
		x, y := a.Nodes[i], b.Nodes[i]
		if x.Role != y.Role || x.Term != y.Term || x.CommitIndex != y.CommitIndex ||
			(x.VotedFor == nil) != (y.VotedFor == nil) ||
			(x.VotedFor != nil && *x.VotedFor != *y.VotedFor) ||
			fmt.Sprint(x.Log) != fmt.Sprint(y.Log) {
			return true
		}
	}
	return false
}

func QueuedRecordsDiffer(a, b State) bool {
	if len(a.InFlight) != len(b.InFlight) {
		return true
	}
	for i := range a.InFlight {
		x, y := a.InFlight[i], b.InFlight[i]
		if x.Type != y.Type || x.From != y.From || x.To != y.To || x.Term != y.Term || x.Data != y.Data || x.DeliverAt != y.DeliverAt {
			return true
		}
	}
	return false
}

func ObservedStateDiffer(a, b State) bool {
	return NodeRecordsDiffer(a, b) || QueuedRecordsDiffer(a, b)
}
