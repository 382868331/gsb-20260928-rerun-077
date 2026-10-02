package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"raftlab/raft"
)

type Lab struct {
	mu          sync.Mutex
	sim         *raft.Simulator
	scenario    raft.Scenario
	experiments map[string]*experiment
}

type experiment struct {
	doc       raft.ExperimentDoc
	sim       *raft.Simulator
	loadError string
}

const experimentDir = "experiments"

type replayRequest struct {
	Scenario *raft.Scenario `json:"scenario"`
}

type saveRequest struct {
	Name     string         `json:"name"`
	Scenario *raft.Scenario `json:"scenario"`
}

func (lab *Lab) currentScenario() raft.Scenario {
	scenario := lab.scenario
	scenario.Actions = lab.sim.Actions()
	return scenario
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func decode(r *http.Request, value any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(value)
}

func recoverMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("handler panic on %s: %v", r.URL.Path, recovered)
				http.Error(w, "internal simulation error; workspace left unchanged", http.StatusInternalServerError)
			}
		}()
		next(w, r)
	}
}

func (lab *Lab) saveExperimentLocked(exp *experiment) error {
	if err := os.MkdirAll(experimentDir, 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(exp.doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(experimentDir, exp.doc.ID+".json"), encoded, 0o644)
}

func (lab *Lab) loadExperiments() {
	lab.experiments = map[string]*experiment{}
	entries, err := os.ReadDir(experimentDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(experimentDir, entry.Name()))
		if err != nil {
			continue
		}
		var doc raft.ExperimentDoc
		if err := json.Unmarshal(raw, &doc); err != nil {
			log.Printf("broken experiment %s kept as record only: %v", entry.Name(), err)
			doc = raft.ExperimentDoc{ID: strings.TrimSuffix(entry.Name(), ".json"), Name: "?",
				Decisions: []raft.Decision{}, Controls: []raft.Control{}}
			lab.experiments[doc.ID] = &experiment{doc: doc, loadError: "实验文件损坏，仅保留记录：" + err.Error()}
			continue
		}
		if doc.Decisions == nil {
			doc.Decisions = []raft.Decision{}
		}
		if doc.Controls == nil {
			doc.Controls = []raft.Control{}
		}
		sim, err := raft.LoadExperiment(doc)
		if err != nil {
			log.Printf("experiment %s kept but not replayable: %v", entry.Name(), err)
			lab.experiments[doc.ID] = &experiment{doc: doc, loadError: "实验数据缺失，无法重放：" + err.Error()}
			continue
		}
		lab.experiments[doc.ID] = &experiment{doc: doc, sim: sim}
	}
}

type experimentView struct {
	raft.ExperimentDoc
	State     *raft.State `json:"state"`
	LoadError string      `json:"loadError,omitempty"`
}

func (exp *experiment) view() experimentView {
	view := experimentView{ExperimentDoc: exp.doc, LoadError: exp.loadError}
	if exp.sim != nil {
		state := exp.sim.Snapshot(300)
		view.State = &state
	}
	return view
}

// recordDecision captures simulator actions appended since before and adds
// them to the branch's auditable decision history.
func (exp *experiment) recordDecision(kind, label string, before int) {
	actions := exp.sim.Actions()
	fresh := append([]raft.Action(nil), actions[before:]...)
	if len(fresh) == 0 {
		return
	}
	exp.doc.Decisions = append(exp.doc.Decisions, raft.Decision{
		ID:      "d" + strconv.Itoa(len(exp.doc.Decisions)+1),
		Kind:    kind,
		Label:   label,
		At:      fresh[0].At,
		Actions: fresh,
	})
}

func (lab *Lab) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/advance", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	mux.HandleFunc("POST /api/client", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	mux.HandleFunc("POST /api/link", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	mux.HandleFunc("POST /api/partition", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	mux.HandleFunc("POST /api/heal", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		lab.sim.HealAll()
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/replay", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body replayRequest
		if err := decode(r, &body); err != nil || body.Scenario == nil {
			http.Error(w, "scenario required", http.StatusBadRequest)
			return
		}
		// Build on a scratch simulator first: a broken replay must never
		// replace (and therefore destroy) the currently open workspace.
		candidate := raft.Replay(*body.Scenario)
		candidate.Snapshot(0)
		lab.mu.Lock()
		lab.scenario = *body.Scenario
		lab.sim = candidate
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("GET /api/scenarios", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		entries, _ := os.ReadDir("scenarios")
		names := []string{"minority-cannot-commit", "old-leader-catches-up"}
		for _, entry := range entries {
			if !entry.IsDir() {
				names = append(names, entry.Name())
			}
		}
		sort.Strings(names[2:])
		writeJSON(w, names)
	}))
	mux.HandleFunc("GET /api/scenario", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		scenario := lab.currentScenario()
		lab.mu.Unlock()
		writeJSON(w, scenario)
	}))
	mux.HandleFunc("POST /api/save", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	mux.HandleFunc("POST /api/arm-pause", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Selector raft.Selector `json:"selector"`
		}
		if err := decode(r, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		lab.sim.ArmPause(body.Selector)
		lab.sim.Advance(1 << 40)
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/clear-pause", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		lab.sim.ClearPause()
		state := lab.sim.Snapshot(300)
		lab.mu.Unlock()
		writeJSON(w, state)
	}))
	mux.HandleFunc("POST /api/fork", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Selector raft.Selector `json:"selector"`
			NameA    string        `json:"nameA"`
			NameB    string        `json:"nameB"`
		}
		if err := decode(r, &body); err != nil || body.Selector.Type == "" {
			http.Error(w, "selector required", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		if !lab.sim.Paused() {
			lab.mu.Unlock()
			http.Error(w, "no active pause point; arm one before forking", http.StatusBadRequest)
			return
		}
		base := lab.currentScenario()
		base.Actions = append(base.Actions, raft.Action{
			Op: "advance", At: lab.sim.Now(), Delay: 1 << 40,
		})
		now := time.Now().UTC().Format(time.RFC3339)
		makeBranch := func(name string) (*experiment, error) {
			stamp := strconv.FormatInt(time.Now().UnixNano(), 36)
			doc := raft.ExperimentDoc{
				ID:        "exp-" + stamp + "-" + name,
				Name:      name,
				Base:      base,
				Decisions: []raft.Decision{},
				Controls:  []raft.Control{},
				CreatedAt: now,
			}
			// Clone the live simulator at the pause point: both branches share
			// the exact same prefix and then diverge independently.
			sim := lab.sim.Clone()
			exp := &experiment{doc: doc, sim: sim}
			lab.experiments[doc.ID] = exp
			if err := lab.saveExperimentLocked(exp); err != nil {
				delete(lab.experiments, doc.ID)
				return nil, err
			}
			return exp, nil
		}
		branchA, err := makeBranch("a")
		if err != nil {
			lab.mu.Unlock()
			http.Error(w, "分叉失败：实验 A 无法保存（暂停点与工作区保持不变）："+err.Error(), http.StatusInternalServerError)
			return
		}
		branchB, err := makeBranch("b")
		if err != nil {
			lab.mu.Unlock()
			http.Error(w, "分叉失败：实验 B 无法保存（实验 A 已保留，暂停点未清除）："+err.Error(), http.StatusInternalServerError)
			return
		}
		viewA := branchA.view()
		viewB := branchB.view()
		lab.mu.Unlock()
		writeJSON(w, map[string]experimentView{"a": viewA, "b": viewB})
	}))
	mux.HandleFunc("GET /api/experiments", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		views := []experimentView{}
		for _, exp := range lab.experiments {
			views = append(views, exp.view())
		}
		lab.mu.Unlock()
		sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
		writeJSON(w, views)
	}))
	mux.HandleFunc("GET /api/experiments/", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len("/api/experiments/"):]
		lab.mu.Lock()
		exp, ok := lab.experiments[id]
		var view experimentView
		if ok {
			view = exp.view()
		}
		lab.mu.Unlock()
		if !ok {
			http.Error(w, "experiment not found", http.StatusNotFound)
			return
		}
		writeJSON(w, view)
	}))
	// mutateBranch runs one audited experiment decision: it snapshots the
	// action log length, applies the request, persists the new decision and
	// returns the updated view. Failures persist nothing.
	mutateBranch := func(w http.ResponseWriter, r *http.Request, mutate func(r *http.Request, exp *experiment) (string, error)) {
		id := r.PathValue("id")
		lab.mu.Lock()
		exp, ok := lab.experiments[id]
		if !ok {
			lab.mu.Unlock()
			http.Error(w, "experiment not found", http.StatusNotFound)
			return
		}
		if exp.sim == nil {
			lab.mu.Unlock()
			http.Error(w, "该实验数据缺失，无法继续操作；其他实验不受影响："+exp.loadError, http.StatusConflict)
			return
		}
		before := len(exp.sim.Actions())
		label, err := mutate(r, exp)
		if err != nil {
			lab.mu.Unlock()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		exp.recordDecision(label, label, before)
		if err := lab.saveExperimentLocked(exp); err != nil {
			lab.mu.Unlock()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		view := exp.view()
		lab.mu.Unlock()
		writeJSON(w, view)
	}
	mux.HandleFunc("POST /api/experiments/{id}/advance", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				Ticks int64 `json:"ticks"`
			}
			if err := decode(r, &body); err != nil {
				return "", err
			}
			start := exp.sim.Now()
			exp.sim.Advance(body.Ticks)
			exp.sim.RecordAdvance(start, exp.sim.Now())
			return "advance", nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/client", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				Node int
				Data string
			}
			if err := decode(r, &body); err != nil {
				return "", err
			}
			if err := exp.sim.Client(body.Node, body.Data); err != nil {
				return "", err
			}
			return "client:" + body.Data, nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/link", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				From, To int
				Up       *bool
				Delay    *int64
			}
			if err := decode(r, &body); err != nil {
				return "", err
			}
			label := "link"
			if body.Up != nil {
				exp.sim.SetLink(body.From, body.To, *body.Up)
				if *body.Up {
					label = "link-up"
				} else {
					label = "link-down"
				}
			}
			if body.Delay != nil {
				if err := exp.sim.SetDelay(body.From, body.To, *body.Delay); err != nil {
					return "", err
				}
				label = "delay"
			}
			return label, nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/partition", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				Groups [][]int `json:"groups"`
			}
			if err := decode(r, &body); err != nil || len(body.Groups) == 0 {
				return "", fmt.Errorf("groups required")
			}
			exp.sim.Partition(body.Groups)
			return "partition", nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/heal", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			exp.sim.HealAll()
			return "heal", nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/deliver-first", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				Selector raft.Selector `json:"selector"`
			}
			if err := decode(r, &body); err != nil {
				return "", err
			}
			if err := exp.sim.DeliverFirst(body.Selector); err != nil {
				return "", err
			}
			return "deliver-first", nil
		})
	}))
	mux.HandleFunc("POST /api/experiments/{id}/postpone", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		mutateBranch(w, r, func(r *http.Request, exp *experiment) (string, error) {
			var body struct {
				Selector raft.Selector `json:"selector"`
				Delay    int64         `json:"delay"`
			}
			if err := decode(r, &body); err != nil {
				return "", err
			}
			if err := exp.sim.Postpone(body.Selector, body.Delay); err != nil {
				return "", err
			}
			return "postpone", nil
		})
	}))
	mux.HandleFunc("GET /api/divergence", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		aID := r.URL.Query().Get("a")
		bID := r.URL.Query().Get("b")
		lab.mu.Lock()
		expA, okA := lab.experiments[aID]
		expB, okB := lab.experiments[bID]
		lab.mu.Unlock()
		if !okA || !okB {
			http.Error(w, "experiment not found", http.StatusNotFound)
			return
		}
		report, err := raft.AnalyzeExperiments(expA.doc, expB.doc)
		if err != nil {
			http.Error(w, "无法分析分歧："+err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, report)
	}))
	// createControl saves a counterfactual: the source branch replayed
	// without one explicitly chosen network intervention, compared against
	// the target track. The saved control is immutable; later branch
	// progress never rewrites it.
	mux.HandleFunc("POST /api/experiments/{id}/controls", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			TargetID   string `json:"targetId"`
			DecisionID string `json:"decisionId"`
		}
		if err := decode(r, &body); err != nil || body.TargetID == "" || body.DecisionID == "" {
			http.Error(w, "targetId and decisionId required", http.StatusBadRequest)
			return
		}
		id := r.PathValue("id")
		lab.mu.Lock()
		exp, ok := lab.experiments[id]
		if !ok {
			lab.mu.Unlock()
			http.Error(w, "experiment not found", http.StatusNotFound)
			return
		}
		target, ok := lab.experiments[body.TargetID]
		if !ok {
			lab.mu.Unlock()
			http.Error(w, "target experiment not found", http.StatusNotFound)
			return
		}
		var chosen *raft.Decision
		for i := range exp.doc.Decisions {
			if exp.doc.Decisions[i].ID == body.DecisionID {
				chosen = &exp.doc.Decisions[i]
				break
			}
		}
		if chosen == nil {
			lab.mu.Unlock()
			http.Error(w, "未找到该干预记录：它不属于这条分支，不能随意归因", http.StatusBadRequest)
			return
		}
		if !raft.IsNetworkIntervention(chosen.Kind) {
			lab.mu.Unlock()
			http.Error(w, "该决定不是网络干预（链路/延迟/分区/恢复），请选择网络干预建立对照", http.StatusBadRequest)
			return
		}
		controlDoc, removed, err := exp.doc.WithoutDecision(body.DecisionID)
		if err != nil {
			lab.mu.Unlock()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := raft.LoadExperiment(controlDoc); err != nil {
			lab.mu.Unlock()
			http.Error(w, "去掉该干预后分支无法重放，对照未保存，原实验未改动："+err.Error(), http.StatusConflict)
			return
		}
		before, err := raft.AnalyzeExperiments(exp.doc, target.doc)
		if err != nil {
			lab.mu.Unlock()
			http.Error(w, "无法分析原轨迹："+err.Error(), http.StatusConflict)
			return
		}
		after, err := raft.AnalyzeExperiments(controlDoc, target.doc)
		if err != nil {
			lab.mu.Unlock()
			http.Error(w, "无法分析对照轨迹："+err.Error(), http.StatusConflict)
			return
		}
		diverges := before.Status == raft.DivergenceConfirmed || before.Status == raft.DivergenceOrderOnly
		control := raft.Control{
			ID:           "ctl-" + strconv.FormatInt(time.Now().UnixNano(), 36),
			CreatedAt:    time.Now().UTC().Format(time.RFC3339),
			SourceID:     exp.doc.ID,
			TargetID:     target.doc.ID,
			DecisionID:   body.DecisionID,
			RemovedLabel: removed.Label,
			ReportBefore: before,
			ReportAfter:  after,
			Explains:     diverges && after.Status == raft.DivergenceIdentical,
		}
		exp.doc.Controls = append(exp.doc.Controls, control)
		if err := lab.saveExperimentLocked(exp); err != nil {
			lab.mu.Unlock()
			http.Error(w, "对照保存失败："+err.Error(), http.StatusInternalServerError)
			return
		}
		view := exp.view()
		lab.mu.Unlock()
		writeJSON(w, view)
	}))
	return mux
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
	log.Fatal(http.ListenAndServe(":8080", lab.routes()))
}
