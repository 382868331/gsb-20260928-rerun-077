package raft

import "testing"

func runTo(e *Engine, tick int) {
	for e.TickValue() < tick {
		e.Tick()
	}
}

func mustLeader(t *testing.T, e *Engine) int {
	t.Helper()
	l := e.currentLeader()
	if l < 0 {
		t.Fatal("expected a leader")
	}
	return l
}

// cut disconnects node from every peer.
func cut(e *Engine, node int) {
	for b := 0; b < e.Size; b++ {
		if b != node {
			if lk := e.links[linkKey(node, b)]; lk != nil {
				lk.Up = false
			}
		}
	}
}

// heal reconnects node to every peer.
func heal(e *Engine, node int) {
	for b := 0; b < e.Size; b++ {
		if b != node {
			if lk := e.links[linkKey(node, b)]; lk != nil {
				lk.Up = true
			}
		}
	}
}

func TestElectsSingleLeader(t *testing.T) {
	e := NewEngine(5, 42)
	runTo(e, 60)
	l := mustLeader(t, e)
	term := e.Nodes[l].CurrentTerm
	leaders := 0
	for _, n := range e.Nodes {
		if n.Role == Leader && n.CurrentTerm == term {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("want exactly 1 leader in term %d, got %d", term, leaders)
	}
}

func TestCommitMajority(t *testing.T) {
	e := NewEngine(5, 7)
	runTo(e, 60)
	l := mustLeader(t, e)
	e.Propose(l, "x=1")
	runTo(e, e.TickValue()+30)

	term := e.Nodes[l].CurrentTerm
	var committed int
	for _, n := range e.Nodes {
		for _, en := range n.Log[1:] {
			if en.Term == term && en.Data == "x=1" && n.CommitIdx >= en.Index {
				committed++
			}
		}
	}
	if committed < 3 {
		t.Fatalf("entry committed on %d nodes, want >= 3", committed)
	}
}

// Partition 2 (old leader + one follower) vs 3 (majority). Minority
// proposals must never commit; the majority elects a new leader.
func TestMinorityCannotCommit(t *testing.T) {
	e := NewEngine(5, 11)
	runTo(e, 60)
	old := mustLeader(t, e)

	follower := -1
	for id := 0; id < 5; id++ {
		if id != old {
			follower = id
			break
		}
	}
	e.Isolate(old)
	e.Isolate(follower)

	e.Propose(old, "minority-write")
	runTo(e, e.TickValue()+80)

	for _, n := range e.Nodes {
		for _, en := range n.Log[1:] {
			if en.Data == "minority-write" && en.Index <= n.CommitIdx {
				t.Fatalf("node %d committed a minority-only entry", n.ID)
			}
		}
	}

	newLeader := -1
	for _, n := range e.Nodes {
		if n.Role == Leader && n.ID != old {
			newLeader = n.ID
		}
	}
	if newLeader < 0 {
		t.Fatal("majority partition failed to elect a new leader")
	}

	e.Propose(newLeader, "majority-write")
	runTo(e, e.TickValue()+30)
	ok := false
	for _, n := range e.Nodes {
		for _, en := range n.Log[1:] {
			if en.Data == "majority-write" && en.Index <= n.CommitIdx {
				ok = true
			}
		}
	}
	if !ok {
		t.Fatal("majority write did not commit after the new election")
	}
}

func TestStaleLeaderHealsAndOverwritesNothing(t *testing.T) {
	e := NewEngine(5, 23)
	runTo(e, 60)
	old := mustLeader(t, e)

	follower := -1
	for id := 0; id < 5; id++ {
		if id != old {
			follower = id
			break
		}
	}
	e.Isolate(old)
	e.Isolate(follower)
	runTo(e, e.TickValue()+80)

	newLeader := -1
	for _, n := range e.Nodes {
		if n.Role == Leader && n.ID != old {
			newLeader = n.ID
		}
	}
	if newLeader < 0 {
		t.Fatal("no new leader in majority")
	}
	e.Propose(newLeader, "committed-after-split")
	runTo(e, e.TickValue()+30)
	e.Propose(old, "stale-write")
	runTo(e, e.TickValue()+30)

	e.Heal(old)
	e.Heal(follower)
	runTo(e, e.TickValue()+80)

	for _, n := range e.Nodes {
		var hasCommitted, hasStale bool
		for _, en := range n.Log[1:] {
			if en.Data == "committed-after-split" && en.Index <= n.CommitIdx {
				hasCommitted = true
			}
			if en.Data == "stale-write" && en.Index <= n.CommitIdx {
				hasStale = true
			}
		}
		if !hasCommitted {
			t.Fatalf("node %d missing committed majority entry after heal", n.ID)
		}
		if hasStale {
			t.Fatalf("node %d committed a stale-leader entry", n.ID)
		}
	}

	minCommit := 1 << 30
	for _, n := range e.Nodes {
		if n.CommitIdx < minCommit {
			minCommit = n.CommitIdx
		}
	}
	for idx := 1; idx <= minCommit; idx++ {
		ref := e.Nodes[0].Log[idx]
		for _, n := range e.Nodes[1:] {
			if n.Log[idx].Term != ref.Term || n.Log[idx].Data != ref.Data {
				t.Fatalf("committed prefix diverges at index %d", idx)
			}
		}
	}
}

func TestReplayDeterministic(t *testing.T) {
	e := NewEngine(5, 99)
	runTo(e, 50)
	l := mustLeader(t, e)
	e.Propose(l, "a")
	runTo(e, e.TickValue()+10)
	e.Isolate(l)
	runTo(e, e.TickValue()+50)
	e.Heal(l)
	runTo(e, e.TickValue()+50)
	e.Propose(0, "b")
	runTo(e, e.TickValue()+20)

	s := e.Scenario("demo")
	e2, ev2 := Replay(s, e.TickValue())

	s1, s2 := e.Snapshot(), e2.Snapshot()
	for i := range s1.Nodes {
		a, b := s1.Nodes[i], s2.Nodes[i]
		if a.Role != b.Role || a.Term != b.Term || a.CommitIdx != b.CommitIdx {
			t.Fatalf("node %d differs after replay: %+v vs %+v", i, a, b)
		}
		if len(a.Log.Entries) != len(b.Log.Entries) {
			t.Fatalf("node %d log length differs", i)
		}
		for k := range a.Log.Entries {
			if a.Log.Entries[k] != b.Log.Entries[k] {
				t.Fatalf("node %d entry %d differs", i, k)
			}
		}
	}
	ev1 := e.Events()
	if len(ev2) != len(ev1) {
		t.Fatalf("event count differs: %d vs %d", len(ev1), len(ev2))
	}
	for i := range ev2 {
		if ev2[i] != ev1[i] {
			t.Fatalf("event %d differs: %+v vs %+v", i, ev1[i], ev2[i])
		}
	}
}
