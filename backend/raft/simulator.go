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
	Paused    bool                 `json:"paused"`
	Pause     *Selector            `json:"pause,omitempty"`
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
	pause           *Selector
	paused          bool
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
		if s.paused && s.matchingQueued() {
			return
		}
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
		if s.pause != nil {
			for _, q := range s.queue {
				if q.m.DeliverAt <= s.time && s.pause.Matches(q.m) {
					s.paused = true
					s.record(Event{Kind: "paused", Detail: selectorDetail(*s.pause)})
					return
				}
			}
		}
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
	state := State{Now: s.time, Links: map[string]LinkState{}, Committed: map[int]string{}, InFlight: []Message{}, Events: []Event{}, Paused: s.paused}
	if s.pause != nil {
		p := *s.pause
		state.Pause = &p
	}
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
	events := append([]Event(nil), s.events...)
	if limit > 0 && len(events) > limit {
		events = events[len(events)-limit:]
	}
	state.Events = events
	return state
}

func (s *Simulator) Events() []Event { return append([]Event(nil), s.events...) }

func (s *Simulator) Now() int64 { return s.time }

// ArmPause stops the next Advance just before the first queued message
// matching sel is delivered. The match is re-evaluated on every queue
// change, so the pause survives message reordering in other branches.
func (s *Simulator) ArmPause(sel Selector) {
	copySel := sel
	s.pause = &copySel
	if s.matchingQueued() {
		s.paused = true
	}
	s.record(Event{Kind: "pause-armed", Detail: selectorDetail(sel)})
	s.actions = append(s.actions, Action{Op: ActionPause, At: s.time,
		MsgType: string(sel.Type), From: sel.From, To: sel.To, Term: sel.Term, Data: sel.Data})
}

func selectorDetail(sel Selector) string {
	return fmt.Sprintf("%s %d->%d term=%d data=%q", sel.Type, sel.From, sel.To, sel.Term, sel.Data)
}

func (s *Simulator) ClearPause() {
	s.pause = nil
	s.paused = false
}

// Paused reports whether a pause selector currently matches a queued
// message and delivery is suspended before it.
func (s *Simulator) Paused() bool { return s.paused }

func (s *Simulator) matchingQueued() bool {
	if s.pause == nil {
		return false
	}
	for _, q := range s.queue {
		if s.pause.Matches(q.m) {
			return true
		}
	}
	return false
}

// PendingMessage describes a message held behind a pause point.
func (s *Simulator) PendingMessages() []Message {
	out := []Message{}
	for _, q := range s.queue {
		out = append(out, q.m)
	}
	return out
}

// DeliverFirst moves the first queued message matching sel to the front of
// the delivery queue, implementing an experiment's reordered delivery.
func (s *Simulator) DeliverFirst(sel Selector) error {
	idx := -1
	for i := range s.queue {
		if sel.Matches(s.queue[i].m) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("no queued message matches selector")
	}
	q := s.queue[idx]
	s.queue = append(s.queue[:idx], s.queue[idx+1:]...)
	s.queue = append([]queuedMessage{q}, s.queue...)
	if idx > 0 && q.m.DeliverAt > s.time {
		q.m.DeliverAt = s.time
		q.m.SendAt = s.time
		s.queue[0] = q
	}
	s.record(Event{Kind: "reorder", Detail: fmt.Sprintf("%s %d->%d first", q.m.Type, q.m.From, q.m.To)})
	s.actions = append(s.actions, Action{Op: ActionDeliverFirst, At: s.time,
		MsgType: string(q.m.Type), From: q.m.From, To: q.m.To, Term: q.m.Term, Data: q.m.Data})
	s.ClearPause()
	return nil
}

// Postpone pushes the first queued message matching sel behind all other
// currently queued messages by the given delay.
func (s *Simulator) Postpone(sel Selector, delay int64) error {
	idx := -1
	for i := range s.queue {
		if sel.Matches(s.queue[i].m) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("no queued message matches selector")
	}
	q := s.queue[idx]
	s.queue = append(s.queue[:idx], s.queue[idx+1:]...)
	last := s.time
	if len(s.queue) > 0 {
		last = s.queue[len(s.queue)-1].m.DeliverAt
	}
	q.m.DeliverAt = last + delay
	s.queue = append(s.queue, q)
	s.record(Event{Kind: "reorder", Detail: fmt.Sprintf("%s %d->%d postponed to %d", q.m.Type, q.m.From, q.m.To, q.m.DeliverAt)})
	s.actions = append(s.actions, Action{Op: ActionPostpone, At: s.time,
		MsgType: string(q.m.Type), From: q.m.From, To: q.m.To, Term: q.m.Term, Data: q.m.Data, Delay: delay})
	s.ClearPause()
	return nil
}

// Clone returns an independent deep copy: both experiments share an
// identical prefix (nodes, logs, links, queue and clock at fork time) and
// diverge independently afterwards.
func (s *Simulator) Clone() *Simulator {
	clone := &Simulator{
		order:           append([]int(nil), s.order...),
		links:           map[string]LinkState{},
		events:          append([]Event(nil), s.events...),
		actions:         append([]Action(nil), s.actions...),
		time:            s.time,
		seed:            s.seed,
		nextMsg:         s.nextMsg,
		lastCommitEvent: map[int]int{},
	}
	clone.nodes = map[int]*Node{}
	for id, n := range s.nodes {
		copied := *n
		copied.Log = &Log{entries: append([]Entry(nil), n.Log.entries...)}
		copied.Peers = append([]int(nil), n.Peers...)
		if n.VotedFor != nil {
			v := *n.VotedFor
			copied.VotedFor = &v
		}
		if n.VotesGranted != nil {
			copied.VotesGranted = map[int]bool{}
			for k, v := range n.VotesGranted {
				copied.VotesGranted[k] = v
			}
		}
		if n.NextIndex != nil {
			copied.NextIndex = map[int]int{}
			for k, v := range n.NextIndex {
				copied.NextIndex[k] = v
			}
		}
		if n.MatchIndex != nil {
			copied.MatchIndex = map[int]int{}
			for k, v := range n.MatchIndex {
				copied.MatchIndex[k] = v
			}
		}
		clone.nodes[id] = &copied
	}
	for key, link := range s.links {
		clone.links[key] = link
	}
	clone.queue = append([]queuedMessage(nil), s.queue...)
	for id, idx := range s.lastCommitEvent {
		clone.lastCommitEvent[id] = idx
	}
	if s.pause != nil {
		p := *s.pause
		clone.pause = &p
		clone.paused = s.paused
	}
	clone.record(Event{Kind: "fork", Detail: fmt.Sprintf("cloned at %d", s.time)})
	return clone
}
