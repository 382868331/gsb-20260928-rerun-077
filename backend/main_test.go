package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"raftlab/raft"
)

func TestExperimentCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	wd, _ := os.Getwd()
	defer os.Chdir(wd)
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	lab := &Lab{sim: raft.NewSimulator([]int{1, 2, 3}, 3), experiments: map[string]*Experiment{}}
	lab.sim.ElectInitial(1)
	lab.sim.Advance(30)
	targets := lab.sim.PeekQueue()
	if len(targets) == 0 {
		t.Fatal("expected queued messages")
	}
	target := targets[0]
	if delta := target.DeliverAt - lab.sim.Now(); delta > 0 {
		lab.sim.Advance(delta - 1)
	}
	root := lab.sim.Checkpoint()
	exp := &Experiment{ID: "exp-1", Label: "trace", MessageID: target.ID,
		MessageType: string(target.Type), From: target.From, To: target.To,
		PausedAt: lab.sim.Now(), CreatedAt: "now", Root: root,
		Branches: map[string]*Branch{
			"a": {Name: "A", Checkpoint: root, Decisions: []Decision{{At: root.Time, Op: "fork"}}},
			"b": {Name: "B", Checkpoint: root, Decisions: []Decision{{At: root.Time, Op: "fork"}}},
		}}
	lab.experiments["exp-1"] = exp
	lab.saveExperiment(exp)

	// Simulate server restart in the same working directory.
	reloaded := &Lab{experiments: map[string]*Experiment{}}
	reloaded.loadExperiments()
	got, ok := reloaded.experiments["exp-1"]
	if !ok {
		t.Fatal("experiment was not reloaded from disk")
	}
	view := reloaded.experimentView(got)
	a := view.Branches["a"].State
	b := view.Branches["b"].State
	if a.Now != b.Now || a.Now != exp.PausedAt {
		t.Fatalf("branches lost common prefix timing: %d %d want %d", a.Now, b.Now, exp.PausedAt)
	}
	if len(a.InFlight) == 0 || a.InFlight[0].ID != target.ID {
		t.Fatal("paused message missing from restored queue")
	}
	if len(view.Branches["a"].Decisions) != 1 {
		t.Fatal("decision trail was not persisted")
	}

	// Mutate one branch, persist, and reload: the other branch and root stay fixed.
	_, err := reloaded.mutateBranch("exp-1", "a", func(sim *raft.Simulator, decisions *[]Decision, now int64) error {
		sim.SetLink(1, 2, false)
		*decisions = append(*decisions, Decision{At: now, Op: "link", Detail: "1->2 down"})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	final := &Lab{experiments: map[string]*Experiment{}}
	final.loadExperiments()
	finalView := final.experimentView(final.experiments["exp-1"])
	if finalView.Branches["a"].State.Links["1->2"].Up {
		t.Fatal("branch A link decision was not persisted")
	}
	if !finalView.Branches["b"].State.Links["1->2"].Up {
		t.Fatal("branch B was contaminated by branch A decision")
	}
	if len(finalView.Branches["a"].Decisions) != 2 {
		t.Fatal("decision trail not appended persistently")
	}
	if _, err := os.Stat(filepath.Join("experiments", "exp-1.json")); err != nil {
		t.Fatal(err)
	}
}

func TestExperimentJSONShape(t *testing.T) {
	sim := raft.NewSimulator([]int{1, 2}, 1)
	sim.ElectInitial(1)
	exp := Experiment{ID: "x", Root: sim.Checkpoint(), Branches: map[string]*Branch{}}
	b, err := json.Marshal(exp)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(b) {
		t.Fatal("invalid json")
	}
}
