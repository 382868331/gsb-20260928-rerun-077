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
	analyses    map[string]*raft.DivergenceAnalysis
	manifests   map[string]*raft.ForkManifest
}

type experiment struct {
	doc     raft.ExperimentDoc
	sim     *raft.Simulator
	loadErr string
}

const (
	experimentDir = "experiments"
	analysisDir   = "analyses"
	forkDir       = "forks"
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

func (lab *Lab) saveManifestLocked(manifest *raft.ForkManifest) error {
	return atomicWriteJSON(forkDir, manifest.ForkKey, manifest)
}

func (lab *Lab) loadManifests() {
	lab.manifests = map[string]*raft.ForkManifest{}
	entries, err := os.ReadDir(forkDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(forkDir, entry.Name()))
		if err != nil {
			continue
		}
		var manifest raft.ForkManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			log.Printf("keep unreadable fork manifest %s: %v", entry.Name(), err)
			continue
		}
		lab.manifests[manifest.ForkKey] = &manifest
	}
}

func (lab *Lab) ensureManifestsLocked() {
	if lab.manifests == nil {
		lab.loadManifests()
	}
}

// forkRecovery describes one damaged pair: which side is still healthy
// (and what it already decided), which side is damaged, and exactly which
// new record a confirmed recovery would create.
type forkRecovery struct {
	HealthySide      string              `json:"healthySide"`
	HealthyID        string              `json:"healthyId"`
	HealthyDecisions []raft.Decision     `json:"healthyDecisions"`
	DamagedSide      string              `json:"damagedSide"`
	DamagedID        string              `json:"damagedId"`
	DamagedError     string              `json:"damagedError"`
	Planned          *raft.ExperimentDoc `json:"planned,omitempty"`
	Recoverable      bool                `json:"recoverable"`
	Reason           string              `json:"reason,omitempty"`
}

type forkView struct {
	ForkKey     string        `json:"forkKey"`
	Phase       string        `json:"phase"`
	SourcePause raft.Selector `json:"sourcePause"`
	SourceAt    int64         `json:"sourceAt"`
	BranchAID   string        `json:"branchAId"`
	BranchBID   string        `json:"branchBId"`
	CreatedAt   string        `json:"createdAt"`
	UpdatedAt   string        `json:"updatedAt"`
	Recovery    *forkRecovery `json:"recovery,omitempty"`
}

// sideHealth checks whether one branch record is present, readable and
// genuinely bound to this fork operation's frozen source snapshot.
func (lab *Lab) sideHealthLocked(manifest *raft.ForkManifest, id string) (*experiment, string) {
	exp := lab.experiments[id]
	if exp == nil {
		exp = lab.experiments[id+".json"]
	}
	if exp == nil {
		return nil, "记录文件缺失"
	}
	if exp.loadErr != "" {
		return nil, exp.loadErr
	}
	if exp.sim == nil || exp.doc.ForkKey != manifest.ForkKey || raft.ScenarioHash(exp.doc.Base) != manifest.SourceHash {
		return nil, "记录与分叉清单不符，不能视为本配对的一侧"
	}
	return exp, ""
}

// classifyLocked inspects a manifest against the loaded experiments and
// derives the recovery preview. It never mutates anything.
func (lab *Lab) classifyLocked(manifest *raft.ForkManifest) *forkRecovery {
	healthyA, errA := lab.sideHealthLocked(manifest, manifest.BranchAID)
	healthyB, errB := lab.sideHealthLocked(manifest, manifest.BranchBID)
	switch {
	case healthyA != nil && healthyB != nil:
		return &forkRecovery{Recoverable: false, Reason: "两侧记录均完好，无需恢复"}
	case healthyA == nil && healthyB == nil:
		return &forkRecovery{Recoverable: false, Reason: "两侧记录都不可用，无法确认健康侧；已保留证据，请人工核对"}
	}
	recovery := &forkRecovery{Recoverable: true}
	var damagedID, damagedName string
	if healthyA != nil {
		recovery.HealthySide = "a"
		recovery.HealthyID = healthyA.doc.ID
		recovery.HealthyDecisions = healthyA.doc.Decisions
		damagedID, damagedName = manifest.BranchBID, "b"
		recovery.DamagedError = errB
	} else {
		recovery.HealthySide = "b"
		recovery.HealthyID = healthyB.doc.ID
		recovery.HealthyDecisions = healthyB.doc.Decisions
		damagedID, damagedName = manifest.BranchAID, "a"
		recovery.DamagedError = errA
	}
	if recovery.HealthyDecisions == nil {
		recovery.HealthyDecisions = []raft.Decision{}
	}
	recovery.DamagedSide = damagedName
	recovery.DamagedID = damagedID
	sourcePause := manifest.SourcePause
	// The rebuilt side starts at the shared pause point with an empty
	// decision list: recovery must never pretend the damaged side
	// executed decisions that were lost with its record.
	recovery.Planned = &raft.ExperimentDoc{
		ID:          damagedID,
		Name:        damagedName,
		Base:        manifest.Base,
		Decisions:   []raft.Decision{},
		SourcePause: &sourcePause,
		SourceAt:    manifest.SourceAt,
		ForkKey:     manifest.ForkKey,
		SourceHash:  manifest.SourceHash,
	}
	return recovery
}

// refreshPhaseLocked recomputes a manifest's phase from the records
// actually present, persisting the transition when it changes. A legacy
// "canceled" tombstone is healed back to the state its records describe:
// cancellation never damages anything and must not permanently block the
// recovery preview after a restart.
func (lab *Lab) refreshPhaseLocked(manifest *raft.ForkManifest) {
	_, errA := lab.sideHealthLocked(manifest, manifest.BranchAID)
	_, errB := lab.sideHealthLocked(manifest, manifest.BranchBID)
	phase := raft.ForkPhasePartial
	if errA == "" && errB == "" {
		phase = raft.ForkPhaseComplete
	}
	if phase != manifest.Phase || manifest.UpdatedAt == "" {
		manifest.Phase = phase
		manifest.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		if err := lab.saveManifestLocked(manifest); err != nil {
			log.Printf("fork %s: phase refresh failed: %v", manifest.ForkKey, err)
		}
	}
}

// activeManifestLocked returns the manifest previously created by this
// exact fork request. One pause point keeps one operation identity for
// life: canceling a recovery session must not let a retried fork mint
// look-alike branches. (The -rN suffix family only survives while
// reading manifests written by older builds.)
func (lab *Lab) activeManifestLocked(baseKey string) (string, *raft.ForkManifest) {
	bestSuffix := -1
	var best *raft.ForkManifest
	for key, manifest := range lab.manifests {
		suffix, ok := forkKeySuffix(baseKey, key)
		if !ok {
			continue
		}
		if suffix > bestSuffix {
			bestSuffix = suffix
			best = manifest
		}
	}
	if best == nil {
		return "", nil
	}
	return best.ForkKey, best
}

// nextForkKeyLocked returns a distinct identity only when the exact key is
// somehow already taken by another manifest family (legacy data); it never
// replaces an unfinished fork of the same request.
func (lab *Lab) nextForkKeyLocked(baseKey string) string {
	maxSuffix := -1
	for key := range lab.manifests {
		if suffix, ok := forkKeySuffix(baseKey, key); ok && suffix > maxSuffix {
			maxSuffix = suffix
		}
	}
	if maxSuffix < 0 {
		return baseKey
	}
	return fmt.Sprintf("%s-r%d", baseKey, maxSuffix+2)
}

func forkKeySuffix(baseKey, key string) (int, bool) {
	if key == baseKey {
		return 0, true
	}
	if !strings.HasPrefix(key, baseKey+"-r") {
		return 0, false
	}
	n, err := strconv.Atoi(key[len(baseKey)+2:])
	if err != nil || n < 2 {
		return 0, false
	}
	return n, true
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
		if strings.Contains(entry.Name(), ".corrupt-") {
			// Quarantined damage evidence, not a loadable experiment.
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
		lab.ensureManifestsLocked()
		if !lab.sim.Paused() {
			lab.mu.Unlock()
			http.Error(w, "no active pause point; arm one before forking", http.StatusBadRequest)
			return
		}
		base := lab.currentScenario()
		base.Actions = append(base.Actions, raft.Action{
			Op: "advance", At: lab.sim.Now(), Delay: 1 << 40,
		})
		baseKey := raft.ForkRequestKey(base, body.Selector, lab.sim.Now())
		if forkKey, manifest := lab.activeManifestLocked(baseKey); manifest != nil {
			lab.refreshPhaseLocked(manifest)
			if existingA, existingB := lab.findForkPairLocked(forkKey); existingA != nil && existingB != nil {
				// Retry of a fork that already succeeded: return the original
				// pair instead of creating look-alike branches.
				viewA := existingA.view()
				viewB := existingB.view()
				lab.mu.Unlock()
				writeJSON(w, map[string]experimentView{"a": viewA, "b": viewB})
				return
			}
			// The operation exists but its pair is incomplete (damaged or
			// missing side). Never mint look-alike branches: direct the
			// user to the recovery preview instead.
			lab.mu.Unlock()
			http.Error(w, "该暂停点的分叉记录不完整，请在“分叉恢复”区域预览并确认恢复；取消恢复只结束本次操作，可再次预览", http.StatusConflict)
			return
		}
		forkKey := lab.nextForkKeyLocked(baseKey)
		now := time.Now().UTC().Format(time.RFC3339)
		manifest := &raft.ForkManifest{
			ForkKey:     forkKey,
			BranchAID:   raft.BranchID(forkKey, "a"),
			BranchBID:   raft.BranchID(forkKey, "b"),
			SourcePause: body.Selector,
			SourceAt:    lab.sim.Now(),
			Base:        base,
			SourceHash:  raft.ScenarioHash(base),
			Phase:       raft.ForkPhasePartial,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		// The manifest is the operation's identity: persist it before any
		// branch record so a crash mid-fork leaves a recoverable trace.
		if err := lab.saveManifestLocked(manifest); err != nil {
			lab.mu.Unlock()
			http.Error(w, "分叉清单保存失败，未创建任何分支："+err.Error(), http.StatusInternalServerError)
			return
		}
		makeBranch := func(name, id string) *experiment {
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
				SourceHash:  manifest.SourceHash,
			}
			// Clone the live simulator at the pause point: both branches share
			// the exact same prefix and then diverge independently.
			sim := lab.sim.Clone()
			exp := &experiment{doc: doc, sim: sim}
			lab.experiments[doc.ID] = exp
			return exp
		}
		branchA := makeBranch("a", manifest.BranchAID)
		branchB := makeBranch("b", manifest.BranchBID)
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
		manifest.Phase = raft.ForkPhaseComplete
		manifest.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		if err := lab.saveManifestLocked(manifest); err != nil {
			log.Printf("fork %s: manifest phase update failed: %v", forkKey, err)
		}
		lab.manifests[forkKey] = manifest
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
	mux.HandleFunc("GET /api/forks", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		lab.mu.Lock()
		lab.ensureManifestsLocked()
		views := []forkView{}
		for _, manifest := range lab.manifests {
			lab.refreshPhaseLocked(manifest)
			view := forkView{
				ForkKey:     manifest.ForkKey,
				Phase:       manifest.Phase,
				SourcePause: manifest.SourcePause,
				SourceAt:    manifest.SourceAt,
				BranchAID:   manifest.BranchAID,
				BranchBID:   manifest.BranchBID,
				CreatedAt:   manifest.CreatedAt,
				UpdatedAt:   manifest.UpdatedAt,
			}
			if manifest.Phase != raft.ForkPhaseComplete {
				// Recovery preview stays available for every unfinished
				// state, including legacy "canceled" manifests healed by
				// refreshPhaseLocked.
				view.Recovery = lab.classifyLocked(manifest)
			}
			views = append(views, view)
		}
		lab.mu.Unlock()
		sort.Slice(views, func(i, j int) bool { return views[i].CreatedAt < views[j].CreatedAt })
		writeJSON(w, views)
	}))
	// Recovery confirmation: rebuild only the damaged side from the fork
	// manifest's frozen base with an empty decision list. The healthy
	// side's record and history are never touched, and the damaged side
	// is never credited with decisions it lost.
	mux.HandleFunc("POST /api/forks/{key}/recover", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		lab.mu.Lock()
		defer lab.mu.Unlock()
		lab.ensureManifestsLocked()
		manifest, ok := lab.manifests[key]
		if !ok {
			http.Error(w, "fork manifest not found", http.StatusNotFound)
			return
		}
		lab.refreshPhaseLocked(manifest)
		if manifest.Phase == raft.ForkPhaseComplete {
			// Repeated confirm (double click, retry after restart): the pair
			// is already intact, return it without rewriting anything.
			expA, expB := lab.experiments[manifest.BranchAID], lab.experiments[manifest.BranchBID]
			if expA != nil && expB != nil {
				writeJSON(w, map[string]experimentView{"a": expA.view(), "b": expB.view()})
				return
			}
		}
		recovery := lab.classifyLocked(manifest)
		if !recovery.Recoverable {
			http.Error(w, recovery.Reason, http.StatusConflict)
			return
		}
		planned := *recovery.Planned
		planned.CreatedAt = time.Now().UTC().Format(time.RFC3339)
		sim, err := raft.LoadExperiment(planned)
		if err != nil {
			http.Error(w, "共同起点无法重建，未改动任何记录："+err.Error(), http.StatusInternalServerError)
			return
		}
		// Quarantine the damaged record as evidence before writing the
		// replacement; never delete it silently.
		damagedPath := filepath.Join(experimentDir, recovery.DamagedID+".json")
		if raw, readErr := os.ReadFile(damagedPath); readErr == nil {
			stamp := strconv.FormatInt(time.Now().UnixNano(), 36)
			evidence := filepath.Join(experimentDir, recovery.DamagedID+".corrupt-"+stamp+".json")
			if writeErr := os.WriteFile(evidence, raw, 0o644); writeErr != nil {
				http.Error(w, "损坏记录取证失败，未改动任何记录："+writeErr.Error(), http.StatusInternalServerError)
				return
			}
		}
		rebuilt := &experiment{doc: planned, sim: sim}
		if err := lab.saveExperimentLocked(rebuilt); err != nil {
			http.Error(w, "恢复记录写入失败，现有记录未改动："+err.Error(), http.StatusInternalServerError)
			return
		}
		delete(lab.experiments, recovery.DamagedID+".json")
		lab.experiments[planned.ID] = rebuilt
		manifest.Phase = raft.ForkPhaseComplete
		manifest.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		if err := lab.saveManifestLocked(manifest); err != nil {
			log.Printf("fork %s: manifest update after recovery failed: %v", key, err)
		}
		expA, expB := lab.experiments[manifest.BranchAID], lab.experiments[manifest.BranchBID]
		writeJSON(w, map[string]experimentView{"a": expA.view(), "b": expB.view()})
	}))
	// Cancel ends only the current recovery session. Nothing on disk is
	// mutated: the unfinished manifest, the damaged record and the healthy
	// side's history all remain, and the same preview can be opened again
	// (including after a restart). The UI hides the card for this session;
	// the server stays stateless about that dismissal.
	mux.HandleFunc("POST /api/forks/{key}/cancel", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		lab.mu.Lock()
		defer lab.mu.Unlock()
		lab.ensureManifestsLocked()
		manifest, ok := lab.manifests[key]
		if !ok {
			http.Error(w, "fork manifest not found", http.StatusNotFound)
			return
		}
		// Recompute the phase from the records present so a legacy canceled
		// tombstone is healed back to a recoverable state on first touch.
		lab.refreshPhaseLocked(manifest)
		view := forkView{
			ForkKey:     manifest.ForkKey,
			Phase:       manifest.Phase,
			SourcePause: manifest.SourcePause,
			SourceAt:    manifest.SourceAt,
			BranchAID:   manifest.BranchAID,
			BranchBID:   manifest.BranchBID,
			CreatedAt:   manifest.CreatedAt,
			UpdatedAt:   manifest.UpdatedAt,
		}
		if manifest.Phase != raft.ForkPhaseComplete {
			view.Recovery = lab.classifyLocked(manifest)
		}
		writeJSON(w, view)
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
	lab.loadManifests()
	lab.loadAnalyses()
	log.Fatal(http.ListenAndServe(":8080", lab.routes()))
}
