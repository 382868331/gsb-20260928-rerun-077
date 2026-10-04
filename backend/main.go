package main

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"raftlab/raft"
)

type Lab struct {
	mu          sync.Mutex
	sim         *raft.Simulator
	scenario    raft.Scenario
	experiments map[string]*experiment
	analyses    map[string]*raft.DivergenceAnalysis
}

type experiment struct {
	doc     raft.ExperimentDoc
	sim     *raft.Simulator
	loadErr string
}

const (
	experimentDir = "experiments"
	analysisDir   = "analyses"
)

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
	w.Header().Set("Cache-Control", "no-store")
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

func atomicWriteJSON(dir, name string, value any) (err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, name+".json")
	tmp, err := os.CreateTemp(dir, ".write-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err = tmp.Write(encoded); err != nil {
		return err
	}
	if err = tmp.Chmod(0o644); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func (lab *Lab) saveExperimentLocked(exp *experiment) error {
	return atomicWriteJSON(experimentDir, exp.doc.ID, exp.doc)
}

func (lab *Lab) saveAnalysisLocked(analysis *raft.DivergenceAnalysis) error {
	return atomicWriteJSON(analysisDir, analysis.ID, analysis)
}

// forkRequestKey identifies one fork operation: the frozen pause state it
// started from (prefix actions, pause selector and clock). Retrying the
// same request maps to the same key, so no look-alike branches appear.
func forkRequestKey(base raft.Scenario, selector raft.Selector, at int64) string {
	sum := sha1.Sum(append(raft.MustJSON(base), raft.MustJSON(selector)...))
	return fmt.Sprintf("%x-%d", hex.EncodeToString(sum[:8]), at)
}

// findForkPairLocked returns the existing a/b pair previously created by
// the same fork request, or nil when this request is new.
func (lab *Lab) findForkPairLocked(key string) (*experiment, *experiment) {
	for _, exp := range lab.experiments {
		if exp.doc.Name != "a" || exp.doc.ForkKey != key || exp.loadErr != "" {
			continue
		}
		var other *experiment
		for _, candidate := range lab.experiments {
			if candidate.doc.Name == "b" && candidate.doc.ForkKey == key && candidate.loadErr == "" {
				other = candidate
				break
			}
		}
		return exp, other
	}
	return nil, nil
}

// recoveryPreview describes a damaged A/B pair before any repair is
// confirmed: the healthy side's executed decisions, the damaged side, the
// common starting point and the exact new record a recovery would create.
type recoveryPreview struct {
	ForkKey        string             `json:"forkKey"`
	Healthy        experimentView     `json:"healthy"`
	DamagedName    string             `json:"damagedName"`
	DamagedState   string             `json:"damagedState"` // "missing" or "damaged"
	DamagedRecords []experimentView   `json:"damagedRecords"`
	SourcePause    *raft.Selector     `json:"sourcePause"`
	SourceAt       int64              `json:"sourceAt"`
	BaseName       string             `json:"baseName"`
	NewRecord      raft.ExperimentDoc `json:"newRecord"`
}

// otherSideName returns the counterpart branch name of an a/b pair.
func otherSideName(name string) string {
	if name == "a" {
		return "b"
	}
	return "a"
}

// healthySiblingLocked returns the healthy counterpart of exp in the same
// fork operation, or nil when that side is missing or unreadable.
func (lab *Lab) healthySiblingLocked(exp *experiment) *experiment {
	want := otherSideName(exp.doc.Name)
	for _, candidate := range lab.experiments {
		if candidate.doc.ForkKey == exp.doc.ForkKey && candidate.doc.Name == want && candidate.loadErr == "" && candidate.sim != nil {
			return candidate
		}
	}
	return nil
}

// damagedRecordsLocked collects the unreadable/unloadable records that may
// be the damaged side of this pair. They are kept listed as evidence and
// are never deleted or overwritten by recovery.
func (lab *Lab) damagedRecordsLocked(key string) []experimentView {
	out := []experimentView{}
	for _, candidate := range lab.experiments {
		if candidate.loadErr == "" {
			continue
		}
		if candidate.doc.ForkKey == key || candidate.doc.Name == "unreadable" {
			out = append(out, candidate.view())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// buildRecoveryLocked computes the recovery plan for the pair that exp
// belongs to. It fails when the pair is already healthy or when exp itself
// cannot serve as the healthy reference side.
func (lab *Lab) buildRecoveryLocked(id string) (recoveryPreview, *experiment, error) {
	exp, ok := lab.experiments[id]
	if !ok {
		return recoveryPreview{}, nil, fmt.Errorf("experiment not found")
	}
	if exp.loadErr != "" || exp.sim == nil {
		return recoveryPreview{}, nil, fmt.Errorf("该记录本身已损坏，不能作为恢复参照；请以健康一侧发起恢复")
	}
	if exp.doc.ForkKey == "" || (exp.doc.Name != "a" && exp.doc.Name != "b") {
		return recoveryPreview{}, nil, fmt.Errorf("该实验不属于可恢复的 A/B 分叉配对")
	}
	if sibling := lab.healthySiblingLocked(exp); sibling != nil {
		return recoveryPreview{}, sibling, nil
	}
	damagedName := otherSideName(exp.doc.Name)
	damaged := lab.damagedRecordsLocked(exp.doc.ForkKey)
	damagedState := "missing"
	if len(damaged) > 0 {
		damagedState = "damaged"
	}
	recoveredFrom := "missing-record"
	if len(damaged) > 0 {
		recoveredFrom = damaged[0].ID
	}
	newDoc := raft.ExperimentDoc{
		ID:            raft.BranchID(exp.doc.ForkKey, damagedName),
		Name:          damagedName,
		Base:          exp.doc.Base,
		Decisions:     []raft.Decision{},
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
		SourcePause:   exp.doc.SourcePause,
		SourceAt:      exp.doc.SourceAt,
		ForkKey:       exp.doc.ForkKey,
		RecoveredFrom: recoveredFrom,
	}
	preview := recoveryPreview{
		ForkKey:        exp.doc.ForkKey,
		Healthy:        exp.view(),
		DamagedName:    damagedName,
		DamagedState:   damagedState,
		DamagedRecords: damaged,
		SourcePause:    exp.doc.SourcePause,
		SourceAt:       exp.doc.SourceAt,
		BaseName:       exp.doc.Base.Name,
		NewRecord:      newDoc,
	}
	return preview, nil, nil
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
			log.Printf("keep unreadable experiment %s: %v", entry.Name(), err)
			lab.experiments[entry.Name()] = &experiment{
				doc:     raft.ExperimentDoc{ID: entry.Name(), Name: "unreadable"},
				loadErr: fmt.Sprintf("实验记录 JSON 已损坏：%v；其他实验仍保留。", err),
			}
			continue
		}
		sim, err := raft.LoadExperiment(doc)
		if err != nil {
			log.Printf("keep unloadable experiment %s: %v", entry.Name(), err)
			lab.experiments[doc.ID] = &experiment{doc: doc, loadErr: fmt.Sprintf("实验无法重建：%v；已保留暂停点和已保存操作。", err)}
			continue
		}
		lab.experiments[doc.ID] = &experiment{doc: doc, sim: sim}
	}
}

func (lab *Lab) loadAnalyses() {
	lab.analyses = map[string]*raft.DivergenceAnalysis{}
	entries, err := os.ReadDir(analysisDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(analysisDir, entry.Name()))
		if err != nil {
			continue
		}
		var analysis raft.DivergenceAnalysis
		if err := json.Unmarshal(raw, &analysis); err != nil {
			log.Printf("skip broken analysis %s: %v", entry.Name(), err)
			continue
		}
		lab.analyses[analysis.ID] = &analysis
	}
}

type experimentView struct {
	raft.ExperimentDoc
	State     *raft.State `json:"state"`
	LoadError string      `json:"loadError,omitempty"`
}

func (exp *experiment) view() experimentView {
	view := experimentView{ExperimentDoc: exp.doc, LoadError: exp.loadErr}
	if view.Decisions == nil {
		// Never emit null: the UI reads decisions as a list and a null
		// here used to crash the whole comparison region.
		view.Decisions = []raft.Decision{}
	}
	if exp.sim != nil {
		state := exp.sim.Snapshot(300)
		view.State = &state
	}
	return view
}

// recordDecision captures simulator actions appended since before and adds
// them to the branch's auditable decision history.
func (exp *experiment) recordDecision(kind, label string, before int) bool {
	actions := exp.sim.Actions()
	fresh := append([]raft.Action(nil), actions[before:]...)
	if len(fresh) == 0 {
		return false
	}
	exp.doc.Decisions = append(exp.doc.Decisions, raft.Decision{
		ID:      "d" + strconv.Itoa(len(exp.doc.Decisions)+1),
		Kind:    kind,
		Label:   label,
		At:      fresh[0].At,
		Actions: fresh,
	})
	return true
}

func findIntervention(doc raft.ExperimentDoc, ref raft.InterventionRef) (raft.Action, error) {
	for _, decision := range doc.Decisions {
		if decision.ID != ref.DecisionID {
			continue
		}
		if ref.ActionIndex < 0 || ref.ActionIndex >= len(decision.Actions) {
			return raft.Action{}, fmt.Errorf("intervention action index out of range")
		}
		return decision.Actions[ref.ActionIndex], nil
	}
	return raft.Action{}, fmt.Errorf("intervention decision not found")
}

func isNetworkIntervention(action raft.Action) bool {
	switch action.Op {
	case raft.ActionDeliverFirst, raft.ActionPostpone, "delay", "link", "partition", "heal":
		return true
	default:
		return false
	}
}

func isReorderDecision(decision raft.Decision) bool {
	for _, action := range decision.Actions {
		if action.Op != raft.ActionDeliverFirst && action.Op != raft.ActionPostpone {
			return false
		}
	}
	return len(decision.Actions) > 0
}

func messageForDecision(decision raft.Decision, sim *raft.Simulator) *raft.Message {
	for _, action := range decision.Actions {
		message := raft.ActionMessage(action)
		if message == nil {
			continue
		}
		if found := raft.FindQueuedMessage(sim, raft.Selector{Type: message.Type, From: message.From, To: message.To, Term: message.Term, Data: message.Data}); found != nil {
			return found
		}
	}
	return nil
}

func messageForPair(doc raft.ExperimentDoc, decision raft.Decision) *raft.Message {
	sim, err := raft.LoadExperimentBeforeDecision(doc, decision.ID)
	if err != nil {
		return nil
	}
	return messageForDecision(decision, sim)
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
		forkKey := forkRequestKey(base, body.Selector, lab.sim.Now())
		if existingA, existingB := lab.findForkPairLocked(forkKey); existingA != nil && existingB != nil {
			// Retry of a fork that already succeeded: return the original
			// pair instead of creating look-alike branches.
			viewA := existingA.view()
			viewB := existingB.view()
			lab.mu.Unlock()
			writeJSON(w, map[string]experimentView{"a": viewA, "b": viewB})
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		makeBranch := func(name string) *experiment {
			stamp := strconv.FormatInt(time.Now().UnixNano(), 36)
			id := "exp-" + stamp + "-" + name
			for suffix := 1; lab.experiments[id] != nil; suffix++ {
				id = fmt.Sprintf("exp-%s-%s-%d", stamp, name, suffix)
			}
			sourcePause := body.Selector
			doc := raft.ExperimentDoc{
				ID:          id,
				Name:        name,
				Base:        base,
				Decisions:   []raft.Decision{},
				CreatedAt:   now,
				SourcePause: &sourcePause,
				SourceAt:    lab.sim.Now(),
				ForkKey:     forkKey,
			}
			// Clone the live simulator at the pause point: both branches share
			// the exact same prefix and then diverge independently.
			sim := lab.sim.Clone()
			exp := &experiment{doc: doc, sim: sim}
			lab.experiments[doc.ID] = exp
			return exp
		}
		branchA := makeBranch("a")
		branchB := makeBranch("b")
		if err := lab.saveExperimentLocked(branchA); err != nil {
			delete(lab.experiments, branchA.doc.ID)
			delete(lab.experiments, branchB.doc.ID)
			lab.mu.Unlock()
			http.Error(w, "分叉 A 保存失败，未保留半成品："+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := lab.saveExperimentLocked(branchB); err != nil {
			_ = os.Remove(filepath.Join(experimentDir, branchA.doc.ID+".json"))
			delete(lab.experiments, branchA.doc.ID)
			delete(lab.experiments, branchB.doc.ID)
			lab.mu.Unlock()
			http.Error(w, "分叉 B 保存失败，已回滚 A，现有实验未改动："+err.Error(), http.StatusInternalServerError)
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
	// Recovery preview: shows what a repair of the damaged pair would do
	// without changing anything. Canceling here keeps every record — the
	// damaged file stays on disk and stays listed as evidence.
	mux.HandleFunc("GET /api/experiments/{id}/recovery-preview", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		lab.mu.Lock()
		preview, sibling, err := lab.buildRecoveryLocked(id)
		lab.mu.Unlock()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if sibling != nil {
			http.Error(w, "配对的另一侧记录健康完整，无需恢复", http.StatusConflict)
			return
		}
		writeJSON(w, preview)
	}))
	// Recovery confirm: creates exactly one replacement record for the
	// damaged side, rebuilt from the common starting point with an empty
	// decision list. The healthy side's file and history are never
	// touched, the damaged record is kept as evidence, and repeating the
	// request returns the already-recovered record instead of duplicating.
	mux.HandleFunc("POST /api/experiments/{id}/recover", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		lab.mu.Lock()
		preview, sibling, err := lab.buildRecoveryLocked(id)
		if err != nil {
			lab.mu.Unlock()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if sibling != nil {
			// Already healthy (repeated click, or recovered earlier and
			// the process restarted): return the existing record.
			view := sibling.view()
			lab.mu.Unlock()
			writeJSON(w, map[string]any{"recovered": false, "existing": true, "experiment": view})
			return
		}
		newDoc := preview.NewRecord
		if existing, ok := lab.experiments[newDoc.ID]; ok && existing.loadErr == "" && existing.sim != nil {
			view := existing.view()
			lab.mu.Unlock()
			writeJSON(w, map[string]any{"recovered": false, "existing": true, "experiment": view})
			return
		}
		sim, loadErr := raft.LoadExperiment(newDoc)
		if loadErr != nil {
			lab.mu.Unlock()
			http.Error(w, "共同起点无法重建，未写入任何恢复记录："+loadErr.Error(), http.StatusInternalServerError)
			return
		}
		recovered := &experiment{doc: newDoc, sim: sim}
		if err := lab.saveExperimentLocked(recovered); err != nil {
			lab.mu.Unlock()
			http.Error(w, "恢复记录保存失败，现有实验未改动："+err.Error(), http.StatusInternalServerError)
			return
		}
		lab.experiments[newDoc.ID] = recovered
		view := recovered.view()
		lab.mu.Unlock()
		writeJSON(w, map[string]any{"recovered": true, "existing": false, "experiment": view})
	}))
	mux.HandleFunc("GET /api/analyses", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		out := make([]raft.DivergenceAnalysis, 0, len(lab.analyses))
		for _, analysis := range lab.analyses {
			out = append(out, *analysis)
		}
		lab.mu.Unlock()
		sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
		writeJSON(w, out)
	}))
	mux.HandleFunc("POST /api/analyses", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ExperimentAID string               `json:"experimentAId"`
			ExperimentBID string               `json:"experimentBId"`
			Candidate     raft.InterventionRef `json:"candidate"`
		}
		if err := decode(r, &body); err != nil || body.ExperimentAID == "" || body.ExperimentBID == "" || body.Candidate.ExperimentID == "" || body.Candidate.DecisionID == "" {
			http.Error(w, "experimentAId、experimentBId 和 candidate.decisionId 均为必填", http.StatusBadRequest)
			return
		}
		lab.mu.Lock()
		defer lab.mu.Unlock()
		expA, okA := lab.experiments[body.ExperimentAID]
		expB, okB := lab.experiments[body.ExperimentBID]
		if !okA || !okB || expA.sim == nil || expB.sim == nil {
			http.Error(w, "实验轨迹缺失或无法重建，不能创建分析；已有记录未改动", http.StatusBadRequest)
			return
		}
		decisionA, decisionB, firstIndex := raft.FirstDifferentDecision(expA.doc, expB.doc)
		if firstIndex < 0 {
			http.Error(w, "两条轨迹尚未出现首次决定分歧", http.StatusBadRequest)
			return
		}
		treated := expA
		opposite := expB
		firstOnTreated := decisionA
		firstOnOpposite := decisionB
		if body.Candidate.ExperimentID == expB.doc.ID {
			treated, opposite = expB, expA
			firstOnTreated, firstOnOpposite = decisionB, decisionA
		} else if body.Candidate.ExperimentID != expA.doc.ID {
			http.Error(w, "所选干预不属于 A/B 中任何一条轨迹", http.StatusBadRequest)
			return
		}
		action, actionErr := findIntervention(treated.doc, body.Candidate)
		classification := "incomplete"
		explanation := "资料尚未完整：选择的干预不在首次决定分歧中，不能据此归因。"
		sufficient, complete := false, false
		var controlDoc raft.ExperimentDoc
		var controlSim, treatedAt, oppositeAt *raft.Simulator
		var messageTreated, messageOpposite *raft.Message
		if body.Candidate.DecisionID == firstOnTreated.ID && isNetworkIntervention(action) && actionErr == nil {
			var err error
			controlDoc, _, err = raft.ExperimentWithoutAction(treated.doc, body.Candidate)
			if err != nil {
				http.Error(w, "建立对照失败："+err.Error(), http.StatusBadRequest)
				return
			}
			controlSim, err = raft.LoadExperimentControlAtDecision(controlDoc, body.Candidate.DecisionID, body.Candidate.ActionIndex)
			if err != nil {
				http.Error(w, "对照无法推进到对应消息："+err.Error(), http.StatusBadRequest)
				return
			}
			treatedAt, err = raft.LoadExperimentAtDecision(treated.doc, firstOnTreated.ID)
			if err != nil {
				http.Error(w, "干预轨迹无法重建到首次分歧："+err.Error(), http.StatusBadRequest)
				return
			}
			oppositeAt, err = raft.LoadExperimentAtDecision(opposite.doc, firstOnOpposite.ID)
			if err != nil {
				http.Error(w, "对照轨迹无法重建到首次分歧："+err.Error(), http.StatusBadRequest)
				return
			}
			treatedPre, err := raft.LoadExperimentBeforeDecision(treated.doc, firstOnTreated.ID)
			if err != nil {
				http.Error(w, "干预轨迹无法定位首次分歧消息："+err.Error(), http.StatusBadRequest)
				return
			}
			oppositePre, err := raft.LoadExperimentBeforeDecision(opposite.doc, firstOnOpposite.ID)
			if err != nil {
				http.Error(w, "对照轨迹无法定位首次分歧消息："+err.Error(), http.StatusBadRequest)
				return
			}
			messageTreated = messageForDecision(firstOnTreated, treatedPre)
			messageOpposite = messageForDecision(firstOnOpposite, oppositePre)
			if messageTreated == nil || messageOpposite == nil {
				classification = "incomplete"
				explanation = "资料尚未完整：首次分歧对应的在途消息已找不到，不能判断是否只是到达顺序不同。"
			} else {
				complete = true
				treatedState := treatedAt.Snapshot(0)
				oppositeState := oppositeAt.Snapshot(0)
				treatedPreState := treatedPre.Snapshot(0)
				oppositePreState := oppositePre.Snapshot(0)
				controlState := controlSim.Snapshot(0)
				if raft.NodeRecordsDiffer(treatedPreState, oppositePreState) {
					classification = "different-acknowledgements"
					explanation = "节点已作出不同确认：首次决定时角色、任期、投票或本地提交记录已经不同。"
				} else if isReorderDecision(firstOnTreated) {
					classification = "arrival-order-only"
					explanation = "仅消息到达顺序不同：节点确认记录尚未分叉。"
				} else {
					classification = "incomplete"
					explanation = "资料尚未完整：当前差异不是可回放的消息顺序或节点确认差异。"
					complete = false
				}
				sufficient = complete && !raft.ObservedStateDiffer(controlState, oppositeState) && raft.ObservedStateDiffer(treatedState, controlState)
				if complete && sufficient {
					explanation += " 移除所选干预后的对照与另一轨迹一致，该干预足以解释本次目标分歧。"
				} else if complete {
					explanation += " 移除所选干预后仍不能得到另一轨迹，单靠该干预不足以解释分歧，不能归因给最后一次操作。"
				}
			}
			if existing := lab.experiments[controlDoc.ID]; existing == nil {
				controlExp := &experiment{doc: controlDoc, sim: controlSim}
				if err := lab.saveExperimentLocked(controlExp); err != nil {
					http.Error(w, "对照保存失败，分析未写入："+err.Error(), http.StatusInternalServerError)
					return
				}
				lab.experiments[controlDoc.ID] = controlExp
			}
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		id := "analysis-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		for lab.analyses[id] != nil {
			id += "-" + strconv.Itoa(len(id))
		}
		var pausePoint *raft.Selector
		if actions := treated.doc.Base.Actions; len(actions) > 0 {
			for _, action := range actions {
				if action.Op == raft.ActionPause {
					sel := raft.Selector{Type: raft.MessageType(action.MsgType), From: action.From, To: action.To, Term: action.Term, Data: action.Data}
					pausePoint = &sel
					break
				}
			}
		}
		analysis := &raft.DivergenceAnalysis{
			ID: id, ExperimentAID: body.ExperimentAID, ExperimentBID: body.ExperimentBID, CreatedAt: now,
			PausePoint: pausePoint, Candidate: body.Candidate, ControlID: controlDoc.ID,
			Classification: classification, Sufficient: sufficient, Complete: complete, Explanation: explanation,
			FirstDecisionA: decisionA, FirstDecisionB: decisionB, MessageA: messageForPair(expA.doc, decisionA),
			MessageB: messageForPair(expB.doc, decisionB), ControlDoc: controlDoc, TreatedDoc: treated.doc,
		}
		if controlSim != nil {
			state := controlSim.Snapshot(300)
			analysis.ControlState = &state
		}
		if treatedAt != nil {
			state := treatedAt.Snapshot(300)
			analysis.TreatedState = &state
		}
		if oppositeAt != nil {
			state := oppositeAt.Snapshot(300)
			analysis.OppositeState = &state
		}
		if err := lab.saveAnalysisLocked(analysis); err != nil {
			http.Error(w, "分析保存失败："+err.Error(), http.StatusInternalServerError)
			return
		}
		lab.analyses[id] = analysis
		writeJSON(w, analysis)
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
		if exp.sim == nil || exp.loadErr != "" {
			lab.mu.Unlock()
			http.Error(w, "该实验记录无法重建，已保留为只读，不能继续推进", http.StatusBadRequest)
			return
		}
		if exp.doc.ReadOnly {
			lab.mu.Unlock()
			http.Error(w, "这是已保存分析的只读对照，继续实验请从原轨迹重新分叉", http.StatusBadRequest)
			return
		}
		before := len(exp.sim.Actions())
		label, err := mutate(r, exp)
		if err != nil {
			lab.mu.Unlock()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !exp.recordDecision(label, label, before) {
			lab.mu.Unlock()
			http.Error(w, "该操作没有产生可记录的实验决定，未保存", http.StatusBadRequest)
			return
		}
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
	lab.loadAnalyses()
	log.Fatal(http.ListenAndServe(":8080", lab.routes()))
}
