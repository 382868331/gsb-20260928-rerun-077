package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"raftlab/raft"
)

type Lab struct {
	mu          sync.Mutex
	sim         *raft.Simulator
	scenario    raft.Scenario
	experiments map[string]*Experiment
	expSeq      int
}

type replayRequest struct {
	Scenario *raft.Scenario `json:"scenario"`
}

type saveRequest struct {
	Name     string         `json:"name"`
	Scenario *raft.Scenario `json:"scenario"`
}

// Decision records one user action on an experiment branch so that every
// choice remains traceable after reloads.
type Decision struct {
	At     int64  `json:"at"`
	Tick   int64  `json:"tick"`
	Op     string `json:"op"`
	Detail string `json:"detail"`
}

type Branch struct {
	Name       string          `json:"name"`
	Checkpoint raft.Checkpoint `json:"checkpoint"`
	Decisions  []Decision      `json:"decisions"`
}

type Experiment struct {
	ID          string             `json:"id"`
	Label       string             `json:"label"`
	MessageID   int                `json:"messageId"`
	MessageType string             `json:"messageType"`
	From        int                `json:"from"`
	To          int                `json:"to"`
	PausedAt    int64              `json:"pausedAt"`
	CreatedAt   string             `json:"createdAt"`
	Root        raft.Checkpoint    `json:"root"`
	Branches    map[string]*Branch `json:"branches"`
}

type BranchView struct {
	Name      string     `json:"name"`
	State     raft.State `json:"state"`
	Decisions []Decision `json:"decisions"`
}

type ExperimentView struct {
	ID          string                `json:"id"`
	Label       string                `json:"label"`
	MessageID   int                   `json:"messageId"`
	MessageType string                `json:"messageType"`
	From        int                   `json:"from"`
	To          int                   `json:"to"`
	PausedAt    int64                 `json:"pausedAt"`
	CreatedAt   string                `json:"createdAt"`
	Branches    map[string]BranchView `json:"branches"`
}

var safeID = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

func (lab *Lab) currentScenario() raft.Scenario {
	scenario := lab.scenario
	scenario.Actions = lab.sim.Actions()
	return scenario
}

func (lab *Lab) saveExperiment(exp *Experiment) {
	if err := os.MkdirAll("experiments", 0o755); err != nil {
		return
	}
	encoded, err := json.MarshalIndent(exp, "", "  ")
	if err != nil {
		return
	}
	name := safeID.ReplaceAllString(exp.ID, "_")
	_ = os.WriteFile("experiments/"+name+".json", encoded, 0o644)
}

func (lab *Lab) loadExperiments() {
	lab.experiments = map[string]*Experiment{}
	entries, err := os.ReadDir("experiments")
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		encoded, err := os.ReadFile("experiments/" + entry.Name())
		if err != nil {
			continue
		}
		var exp Experiment
		if err := json.Unmarshal(encoded, &exp); err != nil || exp.ID == "" {
			continue
		}
		lab.experiments[exp.ID] = &exp
		var seq int
		if _, err := fmt.Sscanf(exp.ID, "exp-%d", &seq); err == nil && seq > lab.expSeq {
			lab.expSeq = seq
		}
	}
}

func (lab *Lab) experimentView(exp *Experiment) ExperimentView {
	view := ExperimentView{ID: exp.ID, Label: exp.Label, MessageID: exp.MessageID,
		MessageType: exp.MessageType, From: exp.From, To: exp.To, PausedAt: exp.PausedAt,
		CreatedAt: exp.CreatedAt, Branches: map[string]BranchView{}}
	for name, branch := range exp.Branches {
		sim := raft.Restore(branch.Checkpoint)
		view.Branches[name] = BranchView{Name: branch.Name, State: sim.Snapshot(300), Decisions: branch.Decisions}
	}
	return view
}

func (lab *Lab) mutateBranch(expID, branchName string, mutate func(*raft.Simulator, *[]Decision, int64) error) (ExperimentView, error) {
	exp, ok := lab.experiments[expID]
	if !ok {
		return ExperimentView{}, fmt.Errorf("unknown experiment %s", expID)
	}
	branch, ok := exp.Branches[branchName]
	if !ok {
		return ExperimentView{}, fmt.Errorf("unknown branch %s", branchName)
	}
	sim := raft.Restore(branch.Checkpoint)
	decisions := append([]Decision(nil), branch.Decisions...)
	if err := mutate(sim, &decisions, sim.Now()); err != nil {
		return ExperimentView{}, err
	}
	branch.Checkpoint = sim.Checkpoint()
	branch.Decisions = decisions
	lab.saveExperiment(exp)
	return lab.experimentView(exp), nil
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func decode(r *http.Request, value any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(value)
}

func main() {
	live := raft.Scenario{
		Name:          "live",
		Description:   "可交互新鲜五节点集群",
		Nodes:         []int{1, 2, 3, 4, 5},
		Seed:          1,
		InitialLeader: 1,
	}
	sim := raft.NewSimulator(live.Nodes, live.Seed)
	_ = sim.ElectInitial(live.InitialLeader)
	lab := &Lab{sim: sim, scenario: live}
	lab.loadExperiments()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	})
	mux.HandleFunc("POST /api/advance", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Ticks int64 `json:"ticks"`
		}
		if err := decode(r, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		start := lab.sim.Now()
		lab.sim.Advance(body.Ticks)
		lab.sim.RecordAdvance(start, lab.sim.Now())
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	})
	mux.HandleFunc("POST /api/client", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Node int
			Data string
		}
		if err := decode(r, &body); err != nil || body.Data == "" {
			http.Error(w, "node and data required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		if err := lab.sim.Client(body.Node, body.Data); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			lab.mu.Unlock()
			return
		}
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	})
	mux.HandleFunc("POST /api/link", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			From, To int
			Up       *bool
			Delay    *int64
		}
		if err := decode(r, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		if body.Up != nil {
			lab.sim.SetLink(body.From, body.To, *body.Up)
		}
		if body.Delay != nil {
			_ = lab.sim.SetDelay(body.From, body.To, *body.Delay)
		}
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	})
	mux.HandleFunc("POST /api/partition", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groups [][]int `json:"groups"`
		}
		if err := decode(r, &body); err != nil || len(body.Groups) == 0 {
			http.Error(w, "groups required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		lab.sim.Partition(body.Groups)
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	})
	mux.HandleFunc("POST /api/heal", func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		lab.sim.HealAll()
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	})
	mux.HandleFunc("POST /api/replay", func(w http.ResponseWriter, r *http.Request) {
		var body replayRequest
		if err := decode(r, &body); err != nil || body.Scenario == nil {
			http.Error(w, "scenario required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		defer lab.mu.Unlock()
		// Replay must never poison the live workspace: a bad scenario stays
		// on the current simulator so later controls keep working.
		var next *raft.Simulator
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					next = nil
				}
			}()
			next = raft.Replay(*body.Scenario)
		}()
		if next == nil {
			http.Error(w, "replay failed; workspace unchanged", http.StatusBadRequest)
			return
		}
		lab.scenario = *body.Scenario
		lab.sim = next
		writeJSON(w, lab.sim.Snapshot(300))
	})
	mux.HandleFunc("GET /api/scenarios", func(w http.ResponseWriter, r *http.Request) {
		entries, _ := os.ReadDir("scenarios")
		names := []string{"minority-cannot-commit", "old-leader-catches-up"}
		for _, entry := range entries {
			if !entry.IsDir() {
				names = append(names, entry.Name())
			}
		}
		sort.Strings(names[2:])
		writeJSON(w, names)
	})
	mux.HandleFunc("GET /api/scenario", func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		scenario := lab.currentScenario()
		lab.mu.Unlock()
		writeJSON(w, scenario)
	})
	mux.HandleFunc("POST /api/save", func(w http.ResponseWriter, r *http.Request) {
		var body saveRequest
		if err := decode(r, &body); err != nil || body.Scenario == nil || body.Name == "" {
			http.Error(w, "name and scenario required", http.StatusBadRequest)
			return
		}
		scenario := lab.currentScenario()
		scenario.Name = body.Name
		lab.scenario = scenario
		if err := os.MkdirAll("scenarios", 0o755); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		encoded, err := json.MarshalIndent(scenario, "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := os.WriteFile("scenarios/"+body.Name+".json", encoded, 0o644); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]bool{"saved": true})
	})
	mux.HandleFunc("POST /api/experiment/fork", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			MessageID int    `json:"messageId"`
			Label     string `json:"label"`
		}
		if err := decode(r, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		defer lab.mu.Unlock()
		var target *raft.Message
		for _, queued := range lab.sim.PeekQueue() {
			queued := queued
			if queued.ID == body.MessageID {
				target = &queued
			}
		}
		if target == nil {
			http.Error(w, fmt.Sprintf("message %d is not queued", body.MessageID), http.StatusBadRequest)
			return
		}
		// Advance the shared timeline up to (but not past) the chosen delivery.
		if delta := target.DeliverAt - lab.sim.Now(); delta > 0 {
			start := lab.sim.Now()
			lab.sim.Advance(delta - 1)
			lab.sim.RecordAdvance(start, lab.sim.Now())
		}
		pausedAt := lab.sim.Now()
		root := lab.sim.Checkpoint()
		lab.expSeq++
		id := fmt.Sprintf("exp-%d", lab.expSeq)
		label := body.Label
		if label == "" {
			label = fmt.Sprintf("%s %d→%d @%d", target.Type, target.From, target.To, target.DeliverAt)
		}
		mkBranch := func(name string) *Branch {
			cp := root
			return &Branch{Name: name, Checkpoint: cp,
				Decisions: []Decision{{At: pausedAt, Tick: 0, Op: "fork", Detail: label}}}
		}
		exp := &Experiment{ID: id, Label: label, MessageID: target.ID,
			MessageType: string(target.Type), From: target.From, To: target.To,
			PausedAt: pausedAt, CreatedAt: time.Now().Format(time.RFC3339), Root: root,
			Branches: map[string]*Branch{"a": mkBranch("A 顺序实验"), "b": mkBranch("B 网络实验")}}
		lab.experiments[id] = exp
		lab.saveExperiment(exp)
		writeJSON(w, lab.experimentView(exp))
	})
	mux.HandleFunc("GET /api/experiments", func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		defer lab.mu.Unlock()
		views := make([]ExperimentView, 0, len(lab.experiments))
		for _, exp := range lab.experiments {
			views = append(views, lab.experimentView(exp))
		}
		sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
		writeJSON(w, views)
	})
	mux.HandleFunc("POST /api/experiment/advance", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Experiment string
			Branch     string
			Ticks      int64 `json:"ticks"`
		}
		if err := decode(r, &body); err != nil || body.Experiment == "" || body.Branch == "" {
			http.Error(w, "experiment and branch required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		defer lab.mu.Unlock()
		view, err := lab.mutateBranch(body.Experiment, body.Branch, func(sim *raft.Simulator, decisions *[]Decision, now int64) error {
			ticks := body.Ticks
			if ticks <= 0 {
				ticks = 50
			}
			sim.Advance(ticks)
			*decisions = append(*decisions, Decision{At: now, Tick: ticks, Op: "advance", Detail: fmt.Sprintf("+%d", ticks)})
			return nil
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, view)
	})
	mux.HandleFunc("POST /api/experiment/reorder", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Experiment string
			Branch     string
			MessageID  int `json:"messageId"`
		}
		if err := decode(r, &body); err != nil || body.Experiment == "" || body.Branch == "" {
			http.Error(w, "experiment and branch required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		defer lab.mu.Unlock()
		view, err := lab.mutateBranch(body.Experiment, body.Branch, func(sim *raft.Simulator, decisions *[]Decision, now int64) error {
			if err := sim.DeliverFirst(body.MessageID); err != nil {
				return err
			}
			*decisions = append(*decisions, Decision{At: now, Op: "reorder", Detail: fmt.Sprintf("message %d first", body.MessageID)})
			return nil
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, view)
	})
	mux.HandleFunc("POST /api/experiment/link", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Experiment string
			Branch     string
			From, To   int
			Up         *bool
			Delay      *int64
		}
		if err := decode(r, &body); err != nil || body.Experiment == "" || body.Branch == "" {
			http.Error(w, "experiment and branch required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		defer lab.mu.Unlock()
		view, err := lab.mutateBranch(body.Experiment, body.Branch, func(sim *raft.Simulator, decisions *[]Decision, now int64) error {
			detail := ""
			if body.Up != nil {
				sim.SetLink(body.From, body.To, *body.Up)
				detail = fmt.Sprintf("link %d→%d up=%v", body.From, body.To, *body.Up)
			}
			if body.Delay != nil {
				if err := sim.SetDelay(body.From, body.To, *body.Delay); err != nil {
					return err
				}
				detail = fmt.Sprintf("link %d→%d delay=%d", body.From, body.To, *body.Delay)
			}
			if detail != "" {
				*decisions = append(*decisions, Decision{At: now, Op: "link", Detail: detail})
			}
			return nil
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, view)
	})
	log.Fatal(http.ListenAndServe(":8080", mux))
}
