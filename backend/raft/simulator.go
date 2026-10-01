package raft

import (
	"fmt"
	"sort"
)

type Event struct {
	VirtualTime int64    `json:"t"`
	Kind        string   `json:"kind"`
	Node        int      `json:"node,omitempty"`
	Term        int      `json:"term,omitempty"`
	Role        Role     `json:"role,omitempty"`
	Message     *Message `json:"message,omitempty"`
	From        int      `json:"from,omitempty"`
	To          int      `json:"to,omitempty"`
	Delay       int64    `json:"delay,omitempty"`
	Index       int      `json:"index,omitempty"`
	Commit      int      `json:"commit,omitempty"`
	Detail      string   `json:"detail,omitempty"`
	Data        string   `json:"data,omitempty"`
}

type NodeView struct {
	ID          int     `json:"id"`
	Role        Role    `json:"role"`
	Term        int     `json:"term"`
	VotedFor    *int    `json:"votedFor"`
	CommitIndex int     `json:"commitIndex"`
	Log         []Entry `json:"log"`
	Deadline    int64   `json:"deadline"`
}

type State struct {
	Now       int64                `json:"now"`
	Nodes     []NodeView           `json:"nodes"`
	Links     map[string]LinkState `json:"links"`
	InFlight  []Message            `json:"inFlight"`
	Events    []Event              `json:"events"`
	Committed map[int]string       `json:"committed"`
}

type LinkState struct {
	From  int   `json:"from"`
	To    int   `json:"to"`
	Delay int64 `json:"delay"`
	Up    bool  `json:"up"`
}

type queuedMessage struct {
	m Message
}

type Simulator struct {
	nodes           map[int]*Node
	order           []int
	links           map[string]LinkState
	queue           []queuedMessage
	events          []Event
	actions         []Action
	time            int64
	seed            uint64
	nextMsg         int
	lastCommitEvent map[int]int
}

func LinkKey(from, to int) string { return fmt.Sprintf("%d->%d", from, to) }

func NewSimulator(ids []int, seed uint64) *Simulator {
	sort.Ints(ids)
	s := &Simulator{nodes: map[int]*Node{}, order: append([]int(nil), ids...),
		links: map[string]LinkState{}, seed: seed, lastCommitEvent: map[int]int{}}
	for _, id := range ids {
		peers := make([]int, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		n := NewNode(id, peers)
		n.resetElection(s.random, 0)
		s.nodes[id] = n
	}
	for _, from := range ids {
		for _, to := range ids {
			if from != to {
				s.links[LinkKey(from, to)] = LinkState{From: from, To: to, Delay: 20, Up: true}
			}
		}
	}
	s.record(Event{Kind: "reset", Detail: fmt.Sprintf("nodes=%d seed=%d", len(ids), seed)})
	return s
}

func (s *Simulator) random(bound int64) int64 {
	s.seed += 0x9E3779B97F4A7C15
	z := s.seed
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	z = z ^ (z >> 31)
	if bound <= 0 {
		return 0
	}
	return int64(z % uint64(bound))
}

func (s *Simulator) record(e Event) {
	e.VirtualTime = s.time
	s.events = append(s.events, e)
}

func (s *Simulator) SetDelay(from, to int, delay int64) error {
	key := LinkKey(from, to)
	link, ok := s.links[key]
	if !ok {
		return fmt.Errorf("unknown link %s", key)
	}
	if delay < 0 {
		delay = 0
	}
	link.Delay = delay
	s.links[key] = link
	s.record(Event{Kind: "delay", From: from, To: to, Delay: delay})
	s.actions = append(s.actions, Action{Op: "delay", At: s.time, From: from, To: to, Delay: delay})
	return nil
}

func (s *Simulator) SetLink(from, to int, up bool) {
	key := LinkKey(from, to)
	link := s.links[key]
	link.Up = up
	s.links[key] = link
	if !up {
		kept := s.queue[:0]
		for _, q := range s.queue {
			if q.m.From == from && q.m.To == to {
				s.record(Event{Kind: "drop", Message: &q.m})
			} else {
				kept = append(kept, q)
			}
		}
		s.queue = kept
	}
	s.record(Event{Kind: map[bool]string{true: "link-up", false: "link-down"}[up], From: from, To: to})
	upValue := up
	s.actions = append(s.actions, Action{Op: "link", At: s.time, From: from, To: to, Up: &upValue})
}

func (s *Simulator) Partition(groups [][]int) {
	set := map[int]map[int]bool{}
	for _, group := range groups {
		members := map[int]bool{}
		for _, id := range group {
			members[id] = true
		}
		for _, id := range group {
			set[id] = members
		}
	}
	for from, link := range s.links {
		up := set[link.From] != nil && set[link.From][link.To]
		link.Up = up
		s.links[from] = link
	}
	kept := s.queue[:0]
	for _, q := range s.queue {
		if !s.links[LinkKey(q.m.From, q.m.To)].Up {
			s.record(Event{Kind: "drop", Message: &q.m})
		} else {
			kept = append(kept, q)
		}
	}
	s.queue = kept
	s.record(Event{Kind: "partition", Detail: fmt.Sprintf("%v", groups)})
	s.actions = append(s.actions, Action{Op: "partition", At: s.time, Groups: groups})
}

func (s *Simulator) HealAll() {
	for key, link := range s.links {
		link.Up = true
		s.links[key] = link
	}
	s.record(Event{Kind: "heal-all"})
	s.actions = append(s.actions, Action{Op: "heal", At: s.time})
}

func (s *Simulator) ElectInitial(id int) error {
	n, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("unknown node %d", id)
	}
	n.Role = Leader
	n.Term = 1
	n.VotedFor = &id
	n.NextIndex = map[int]int{}
	n.MatchIndex = map[int]int{id: 0}
	for _, peer := range n.Peers {
		n.NextIndex[peer] = 1
		n.MatchIndex[peer] = 0
	}
	n.HeartbeatDue = HeartbeatInterval
	n.broadcastAppend(s.send)
	s.record(Event{Kind: "role", Node: id, Role: Leader, Term: 1})
	return nil
}

func (s *Simulator) Client(node int, data string) error {
	n, ok := s.nodes[node]
	if !ok {
		return fmt.Errorf("unknown node %d", node)
	}
	m := Message{Type: MsgClient, From: node, To: node, Data: data}
	s.deliverNow(n, m)
	s.actions = append(s.actions, Action{Op: "client", At: s.time, Node: node, Data: data})
	return nil
}

func (s *Simulator) Actions() []Action { return append([]Action(nil), s.actions...) }

func (s *Simulator) RecordAdvance(oldTime, newTime int64) {
	s.actions = append(s.actions, Action{Op: ActionAdvance, At: oldTime, Delay: newTime - oldTime})
}

func (s *Simulator) send(m Message) {
	if m.Type == MsgClient {
		return
	}
	link, ok := s.links[LinkKey(m.From, m.To)]
	m.ID = s.nextMsg
	s.nextMsg++
	m.SendAt = s.time
	if !ok || !link.Up {
		s.record(Event{Kind: "drop", Message: &m})
		return
	}
	m.DeliverAt = s.time + link.Delay
	s.queue = append(s.queue, queuedMessage{m: m})
	sort.SliceStable(s.queue, func(i, j int) bool { return s.queue[i].m.DeliverAt < s.queue[j].m.DeliverAt })
	s.record(Event{Kind: "send", Message: &m, Delay: link.Delay})
}

func (s *Simulator) deliverNow(n *Node, m Message) {
	beforeRole, beforeTerm := n.Role, n.Term
	notes := n.Handle(m, s.random, s.time, s.send)
	if n.Role != beforeRole || n.Term != beforeTerm {
		s.record(Event{Kind: "role", Node: n.ID, Role: n.Role, Term: n.Term})
	}
	s.record(Event{Kind: "receive", Node: n.ID, Message: &m})
	for _, note := range notes {
		s.record(Event{Kind: "note", Node: n.ID, Term: n.Term, Detail: note, Data: m.Data})
	}
	s.recordCommits(n)
}

func (s *Simulator) recordCommits(n *Node) {
	for index := s.lastCommitEvent[n.ID] + 1; index <= n.CommitIndex; index++ {
		term, ok := n.Log.termAt(index)
		if !ok {
			continue
		}
		entry := n.Log.entries[index]
		s.record(Event{Kind: "commit", Node: n.ID, Term: term, Index: index, Commit: n.CommitIndex, Data: entry.Data})
		s.lastCommitEvent[n.ID] = index
	}
}

func (s *Simulator) Advance(delta int64) {
	end := s.time + delta
	for s.time < end {
		next := end
		for _, id := range s.order {
			n := s.nodes[id]
			deadline := n.ElectionDeadline
			if n.Role == Leader {
				deadline = n.HeartbeatDue
			}
			if deadline < next {
				next = deadline
			}
		}
		if len(s.queue) > 0 && s.queue[0].m.DeliverAt < next {
			next = s.queue[0].m.DeliverAt
		}
		if next > end {
			next = end
		}
		s.time = next
		for len(s.queue) > 0 && s.queue[0].m.DeliverAt <= s.time {
			q := s.queue[0]
			s.queue = s.queue[1:]
			if s.links[LinkKey(q.m.From, q.m.To)].Up {
				s.deliverNow(s.nodes[q.m.To], q.m)
			} else {
				s.record(Event{Kind: "drop", Message: &q.m})
			}
		}
		for _, id := range s.order {
			n := s.nodes[id]
			if n.TimeoutFired(s.time) {
				beforeRole, beforeTerm := n.Role, n.Term
				note := n.HandleTimeout(s.random, s.time, s.send)
				if n.Role != beforeRole || n.Term != beforeTerm {
					s.record(Event{Kind: "role", Node: n.ID, Role: n.Role, Term: n.Term})
				}
				s.record(Event{Kind: "timeout", Node: n.ID, Term: n.Term, Detail: note})
				s.recordCommits(n)
			}
		}
	}
}

func (s *Simulator) Snapshot(limit int) State {
	state := State{Now: s.time, Links: map[string]LinkState{}, Committed: map[int]string{}}
	state.InFlight = []Message{}
	state.Events = []Event{}
	for _, id := range s.order {
		n := s.nodes[id]
		deadline := n.ElectionDeadline
		if n.Role == Leader {
			deadline = n.HeartbeatDue
		}
		state.Nodes = append(state.Nodes, NodeView{ID: n.ID, Role: n.Role, Term: n.Term, VotedFor: n.VotedFor,
			CommitIndex: n.CommitIndex, Log: n.Log.snapshot(), Deadline: deadline})
	}
	for key, value := range s.links {
		state.Links[key] = value
	}
	for _, q := range s.queue {
		state.InFlight = append(state.InFlight, q.m)
	}
	maxLen := 0
	for _, node := range state.Nodes {
		if len(node.Log) > maxLen {
			maxLen = len(node.Log)
		}
	}
	for index := 1; index <= maxLen; index++ {
		data := ""
		agreement, votes := 0, 0
		for _, node := range state.Nodes {
			if node.CommitIndex < index || len(node.Log) < index {
				continue
			}
			entry := node.Log[index-1]
			if entry.Index != index {
				continue
			}
			if votes == 0 {
				data = entry.Data
				agreement = 1
			} else if entry.Data == data {
				agreement++
			}
			votes++
		}
		if agreement*2 > len(state.Nodes) {
			state.Committed[index] = data
		}
	}
	events := s.events
	if limit > 0 && len(events) > limit {
		events = events[len(events)-limit:]
	}
	state.Events = append([]Event(nil), events...)
	return state
}

func (s *Simulator) Events() []Event { return append([]Event(nil), s.events...) }

func (s *Simulator) Now() int64 { return s.time }

// Checkpoint captures the complete mutable state of a simulator so that an
// experiment branch can diverge without affecting other branches.
type Checkpoint struct {
	Nodes   map[int]NodeCheckpoint `json:"nodes"`
	Order   []int                  `json:"order"`
	Links   map[string]LinkState   `json:"links"`
	Queue   []Message              `json:"queue"`
	Events  []Event                `json:"events"`
	Actions []Action               `json:"actions"`
	Time    int64                  `json:"time"`
	Seed    uint64                 `json:"seed"`
	NextMsg int                    `json:"nextMsg"`
	Marks   map[int]int            `json:"marks"`
}

type NodeCheckpoint struct {
	Node             Node         `json:"node"`
	Entries          []Entry      `json:"entries"`
	ElectionDeadline int64        `json:"electionDeadline"`
	HeartbeatDue     int64        `json:"heartbeatDue"`
	VotesGranted     map[int]bool `json:"votesGranted"`
	NextIndex        map[int]int  `json:"nextIndex"`
	MatchIndex       map[int]int  `json:"matchIndex"`
}

func (s *Simulator) Checkpoint() Checkpoint {
	c := Checkpoint{
		Nodes: map[int]NodeCheckpoint{}, Links: map[string]LinkState{},
		Queue: []Message{}, Events: append([]Event(nil), s.events...),
		Actions: append([]Action(nil), s.actions...),
		Time:    s.time, Seed: s.seed, NextMsg: s.nextMsg,
		Marks: map[int]int{},
	}
	c.Order = append(c.Order, s.order...)
	for key, link := range s.links {
		c.Links[key] = link
	}
	for _, q := range s.queue {
		c.Queue = append(c.Queue, q.m)
	}
	for id, n := range s.nodes {
		var votedFor *int
		if n.VotedFor != nil {
			v := *n.VotedFor
			votedFor = &v
		}
		nc := NodeCheckpoint{
			Node: Node{ID: n.ID, Role: n.Role, Term: n.Term, VotedFor: votedFor,
				CommitIndex: n.CommitIndex, Peers: append([]int(nil), n.Peers...)},
			Entries:          append([]Entry(nil), n.Log.entries...),
			ElectionDeadline: n.ElectionDeadline, HeartbeatDue: n.HeartbeatDue,
			VotesGranted: map[int]bool{}, NextIndex: map[int]int{}, MatchIndex: map[int]int{},
		}
		for k, v := range n.VotesGranted {
			nc.VotesGranted[k] = v
		}
		for k, v := range n.NextIndex {
			nc.NextIndex[k] = v
		}
		for k, v := range n.MatchIndex {
			nc.MatchIndex[k] = v
		}
		c.Nodes[id] = nc
	}
	for k, v := range s.lastCommitEvent {
		c.Marks[k] = v
	}
	return c
}

func Restore(c Checkpoint) *Simulator {
	s := &Simulator{
		nodes: map[int]*Node{}, order: append([]int(nil), c.Order...),
		links: map[string]LinkState{}, queue: []queuedMessage{},
		events: append([]Event(nil), c.Events...), actions: append([]Action(nil), c.Actions...),
		time: c.Time, seed: c.Seed, nextMsg: c.NextMsg, lastCommitEvent: map[int]int{},
	}
	for key, link := range c.Links {
		s.links[key] = link
	}
	for _, m := range c.Queue {
		s.queue = append(s.queue, queuedMessage{m: m})
	}
	for id, nc := range c.Nodes {
		n := &Node{
			ID: nc.Node.ID, Role: nc.Node.Role, Term: nc.Node.Term, VotedFor: nc.Node.VotedFor,
			CommitIndex: nc.Node.CommitIndex, Peers: append([]int(nil), nc.Node.Peers...),
			Log:              &Log{entries: append([]Entry(nil), nc.Entries...)},
			ElectionDeadline: nc.ElectionDeadline, HeartbeatDue: nc.HeartbeatDue,
			VotesGranted: map[int]bool{}, NextIndex: map[int]int{}, MatchIndex: map[int]int{},
		}
		for k, v := range nc.VotesGranted {
			n.VotesGranted[k] = v
		}
		for k, v := range nc.NextIndex {
			n.NextIndex[k] = v
		}
		for k, v := range nc.MatchIndex {
			n.MatchIndex[k] = v
		}
		s.nodes[id] = n
	}
	for k, v := range c.Marks {
		s.lastCommitEvent[k] = v
	}
	return s
}

func (s *Simulator) Clone() *Simulator { return Restore(s.Checkpoint()) }

// PeekQueue returns the messages that are still waiting to be delivered,
// ordered by delivery time.
func (s *Simulator) PeekQueue() []Message {
	out := make([]Message, len(s.queue))
	for i, q := range s.queue {
		out[i] = q.m
	}
	return out
}

// DeliverFirst moves the message with the given ID to the front of the queue
// while preserving the delivery times of all other messages. Messages sharing
// the same delivery time are delivered in queue order, so this changes the
// delivery ordering without inventing a new time.
func (s *Simulator) DeliverFirst(id int) error {
	idx := -1
	for i, q := range s.queue {
		if q.m.ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("message %d is not queued", id)
	}
	q := s.queue[idx]
	q.m.DeliverAt = s.time
	s.queue[idx] = q
	sort.SliceStable(s.queue, func(i, j int) bool { return s.queue[i].m.DeliverAt < s.queue[j].m.DeliverAt })
	s.record(Event{Kind: "reorder", Message: &q.m, Detail: fmt.Sprintf("message %d first at %d", id, s.time)})
	s.actions = append(s.actions, Action{Op: "reorder", At: s.time, MsgID: &id})
	return nil
}
