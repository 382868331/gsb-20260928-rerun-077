package raft

import (
	"math/rand"
	"sort"
)

// Action is a recorded external input. Replaying the same seed plus the
// same action sequence reproduces the simulation exactly.
type Action struct {
	Tick     int    `json:"tick"`
	Kind     string `json:"kind"` // propose, partition, heal, setDelay, reset
	Data     string `json:"data,omitempty"`
	ClientID string `json:"clientId,omitempty"`
	A        int    `json:"a,omitempty"`
	B        int    `json:"b,omitempty"`
	Side     string `json:"side,omitempty"` // isolate: which side node id joins
	DelayMin int    `json:"delayMin,omitempty"`
	DelayMax int    `json:"delayMax,omitempty"`
	LossPct  int    `json:"lossPct,omitempty"`
	Nodes    int    `json:"nodes,omitempty"`
	Seed     int    `json:"seed,omitempty"`
}

// Event is an observable thing that happened, emitted on the timeline.
type Event struct {
	Tick int    `json:"tick"`
	Kind string `json:"kind"`
	A    int    `json:"a"`
	B    int    `json:"b"`
	Term int    `json:"term"`
	Text string `json:"text"`
}

type LinkState struct {
	Up       bool `json:"up"`
	DelayMin int  `json:"delayMin"`
	DelayMax int  `json:"delayMax"`
	LossPct  int  `json:"lossPct"`
}

type LogView struct {
	Entries []Entry `json:"entries"` // without sentinel
	Commit  int     `json:"commit"`
}

type NodeSnapshot struct {
	ID        int     `json:"id"`
	Role      Role    `json:"role"`
	Term      int     `json:"term"`
	VotedFor  int     `json:"votedFor"`
	LeaderID  int     `json:"leaderId"`
	CommitIdx int     `json:"commitIdx"`
	Log       LogView `json:"log"`
}

type Snapshot struct {
	Tick   int                  `json:"tick"`
	Nodes  []NodeSnapshot       `json:"nodes"`
	Links  map[string]LinkState `json:"links"`
	Leader int                  `json:"leader"` // inferred node claiming leadership of the majority term, -1 otherwise
	Quorum int                  `json:"quorum"`
}

type inFlight struct {
	m       Message
	deliver int
}

type Engine struct {
	Size      int
	Seed      int
	Nodes     []*Node
	tick      int
	links     map[string]*LinkState
	queue     []inFlight
	actions   []Action
	events    []Event
	pending   []Action // actions scheduled for future ticks
	clients   map[string]int
	clientSeq int
	netRng    []*rand.Rand
}

func NewEngine(size, seed int) *Engine {
	e := &Engine{
		Size:    size,
		Seed:    seed,
		links:   map[string]*LinkState{},
		clients: map[string]int{},
	}
	e.reset(size, seed)
	return e
}

func (e *Engine) reset(size, seed int) {
	e.Size = size
	e.Seed = seed
	e.Nodes = make([]*Node, size)
	for i := range e.Nodes {
		e.Nodes[i] = newNode(i, size, seed)
	}
	e.tick = 0
	e.queue = nil
	e.actions = nil
	e.events = nil
	e.pending = nil
	e.clients = map[string]int{}
	e.netRng = make([]*rand.Rand, size)
	for i := range e.netRng {
		e.netRng[i] = rand.New(rand.NewSource(int64(seed)*7919 + int64(i+1)))
	}
	e.links = map[string]*LinkState{}
	for a := 0; a < size; a++ {
		for b := a + 1; b < size; b++ {
			e.links[linkKey(a, b)] = &LinkState{Up: true, DelayMin: 1, DelayMax: 2, LossPct: 0}
		}
	}
}

func linkKey(a, b int) string {
	if a > b {
		a, b = b, a
	}
	return string(rune('0'+a)) + "-" + string(rune('0'+b))
}

func (e *Engine) Tick() int {
	e.tick++
	t := e.tick

	// 1. Time-driven behaviour (elections, heartbeats).
	for _, n := range e.Nodes {
		for _, m := range n.Tick(t, e.Size) {
			e.send(m)
		}
	}

	// 2. Deliver due messages, collecting replies; process in stable
	// (type, from, to) order for determinism.
	due := []Message{}
	waiting := e.queue[:0]
	for _, f := range e.queue {
		if f.deliver <= t {
			due = append(due, f.m)
		} else {
			waiting = append(waiting, f)
		}
	}
	e.queue = waiting
	sort.SliceStable(due, func(i, j int) bool {
		if due[i].From != due[j].From {
			return due[i].From < due[j].From
		}
		if due[i].To != due[j].To {
			return due[i].To < due[j].To
		}
		return due[i].Type < due[j].Type
	})

	for _, m := range due {
		if m.Type == "clientAck" {
			e.handleClientAck(m)
			continue
		}
		if !e.linkUp(m.From, m.To) {
			continue // link healed/redropped while in flight
		}
		replies := e.Nodes[m.To].Step(m, t, e.Size)
		for _, r := range replies {
			e.send(r)
		}
	}

	// 3. Scheduled replay actions recorded at tick t fire at the end of
	// tick t, matching a live user clicking between ticks: their effect
	// (and any client messages) lands before tick t+1's timers.
	remaining := e.pending[:0]
	for _, act := range e.pending {
		if act.Tick <= t {
			e.applyAction(act)
		} else {
			remaining = append(remaining, act)
		}
	}
	e.pending = remaining

	return t
}

func (e *Engine) linkUp(a, b int) bool {
	if a == b || a < 0 || b < 0 || a >= e.Size || b >= e.Size {
		return true
	}
	l, ok := e.links[linkKey(a, b)]
	return ok && l.Up
}

func (e *Engine) send(m Message) {
	m.AtTick = e.tick
	if m.Type == "clientAck" {
		e.queue = append(e.queue, inFlight{m: m, deliver: e.tick})
		return
	}
	if m.From == m.To {
		// Internal self-addressed messages (self vote) process immediately
		// at the next delivery phase of this same tick.
		e.queue = append(e.queue, inFlight{m: m, deliver: e.tick})
		return
	}
	l := e.links[linkKey(m.From, m.To)]
	if l == nil || !l.Up {
		e.emit(Event{Tick: e.tick, Kind: "drop", A: m.From, B: m.To, Term: m.Term,
			Text: m.Type + " dropped (partitioned)"})
		return
	}
	delay := l.DelayMin
	if l.DelayMax > l.DelayMin {
		delay += e.rngFor(m.From).Intn(l.DelayMax - l.DelayMin + 1)
	}
	if l.LossPct > 0 && e.rngFor(m.From).Intn(100) < l.LossPct {
		e.emit(Event{Tick: e.tick, Kind: "drop", A: m.From, B: m.To, Term: m.Term,
			Text: m.Type + " lost"})
		return
	}
	e.queue = append(e.queue, inFlight{m: m, deliver: e.tick + delay})
}

// rngFor is used only for link delay/loss, kept per-node and derived
// from the scenario seed for replay determinism.
func (e *Engine) rngFor(id int) *rand.Rand { return e.netRng[id] }

func (e *Engine) emit(ev Event) {
	if ev.Tick == 0 {
		ev.Tick = e.tick
	}
	e.events = append(e.events, ev)
}

// -------- external API --------

// Propose queues a client write against node target. Returns the client
// correlation id. Success/failure arrives as timeline events.
func (e *Engine) Propose(target int, data string) string {
	e.clientSeq++
	id := clientID(e.clientSeq)
	act := Action{Tick: e.tick, Kind: "propose", A: target, Data: data, ClientID: id}
	e.actions = append(e.actions, act)
	e.applyAction(act)
	return id
}

// Partition cuts all links between the two groups: side contains node id,
// the other side is everyone else. dir isolates node Side from all.
func (e *Engine) Isolate(node int) {
	act := Action{Tick: e.tick, Kind: "isolate", A: node}
	e.actions = append(e.actions, act)
	e.applyAction(act)
}

func (e *Engine) Heal(node int) {
	act := Action{Tick: e.tick, Kind: "heal", A: node}
	e.actions = append(e.actions, act)
	e.applyAction(act)
}

// SetLink adjusts delay/loss for a link (a<0 means all links).
func (e *Engine) SetLink(a, b, dmin, dmax, loss int) {
	act := Action{Tick: e.tick, Kind: "setLink", A: a, B: b, DelayMin: dmin, DelayMax: dmax, LossPct: loss}
	e.actions = append(e.actions, act)
	e.applyAction(act)
}

func (e *Engine) Reconfigure(size, seed int) {
	act := Action{Tick: e.tick, Kind: "reset", Nodes: size, Seed: seed}
	e.actions = append(e.actions, act)
	e.applyAction(act)
}

// ScheduleReplayAction stores an action to fire at its recorded tick.
func (e *Engine) ScheduleReplayAction(a Action) {
	e.pending = append(e.pending, a)
}

func (e *Engine) applyAction(a Action) {
	switch a.Kind {
	case "propose":
		id := a.ClientID
		if id == "" {
			e.clientSeq++
			id = clientID(e.clientSeq)
		}
		e.clients[id] = a.A
		e.emit(Event{Kind: "propose", A: a.A, Text: "client → node" + itoa(a.A) + ": \"" + a.Data + "\""})
		msgs := e.Nodes[a.A].Step(Message{Type: "client", From: -1, ClientID: a.ClientID, Data: a.Data}, e.tick, e.Size)
		for _, m := range msgs {
			e.send(m)
		}
	case "isolate":
		for b := 0; b < e.Size; b++ {
			if b != a.A {
				if l := e.links[linkKey(a.A, b)]; l != nil {
					l.Up = false
				}
			}
		}
		e.emit(Event{Kind: "partition", A: a.A, Text: "node" + itoa(a.A) + " partitioned from cluster"})
	case "heal":
		for b := 0; b < e.Size; b++ {
			if b != a.A {
				if l := e.links[linkKey(a.A, b)]; l != nil {
					l.Up = true
				}
			}
		}
		e.emit(Event{Kind: "heal", A: a.A, Text: "node" + itoa(a.A) + " reconnected"})
	case "setLink":
		for key, l := range e.links {
			if a.A < 0 || key == linkKey(a.A, a.B) {
				l.DelayMin, l.DelayMax, l.LossPct = a.DelayMin, a.DelayMax, a.LossPct
			}
		}
	case "reset":
		e.reset(a.Nodes, a.Seed)
		e.emit(Event{Kind: "reset", Text: "cluster reset"})
	}
}
