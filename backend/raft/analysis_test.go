package raft

import "testing"

func TestCompareTracksConfirmedDivergence(t *testing.T) {
	a := Track{Confirmations: []Confirmation{{At: 10, Node: 2, Index: 1, Term: 1, Data: "x"}}}
	b := Track{Confirmations: []Confirmation{{At: 10, Node: 2, Index: 1, Term: 1, Data: "y"}}}
	report := CompareTracks(a, b)
	if report.Status != DivergenceConfirmed {
		t.Fatalf("status = %s, want confirmed", report.Status)
	}
	if report.Node != 2 || report.Index != 1 || report.DataA != "x" || report.DataB != "y" {
		t.Fatalf("anchors wrong: %+v", report)
	}
}

func TestCompareTracksOrderOnly(t *testing.T) {
	shared := []Confirmation{{At: 10, Node: 1, Index: 1, Term: 1, Data: "x"}}
	a := Track{
		Confirmations: shared,
		Deliveries: []Delivery{
			{At: 1, Type: MsgAppend, From: 1, To: 2, Term: 1},
			{At: 2, Type: MsgAppend, From: 1, To: 3, Term: 1},
		},
	}
	b := Track{
		Confirmations: shared,
		Deliveries: []Delivery{
			{At: 2, Type: MsgAppend, From: 1, To: 3, Term: 1},
			{At: 1, Type: MsgAppend, From: 1, To: 2, Term: 1},
		},
	}
	report := CompareTracks(a, b)
	if report.Status != DivergenceOrderOnly {
		t.Fatalf("status = %s, want order-only", report.Status)
	}
	if report.DeliveryA == nil || report.DeliveryA.To != 2 || report.DeliveryB == nil || report.DeliveryB.To != 3 {
		t.Fatalf("delivery anchors wrong: %+v", report)
	}
}

func TestCompareTracksIdentical(t *testing.T) {
	track := Track{
		Confirmations: []Confirmation{{At: 10, Node: 1, Index: 1, Term: 1, Data: "x"}},
		Deliveries:    []Delivery{{At: 1, Type: MsgAppend, From: 1, To: 2, Term: 1}},
	}
	if report := CompareTracks(track, track); report.Status != DivergenceIdentical {
		t.Fatalf("status = %s, want identical", report.Status)
	}
}

func TestAnalyzeExperimentsInsufficientWithoutDecisions(t *testing.T) {
	base := Scenario{Name: "live", Nodes: []int{1, 2, 3}, Seed: 9, InitialLeader: 1}
	docA := ExperimentDoc{ID: "a", Base: base}
	docB := ExperimentDoc{ID: "b", Base: base, Decisions: []Decision{{ID: "d1", Kind: "heal"}}}
	report, err := AnalyzeExperiments(docA, docB)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != DivergenceInsufficient {
		t.Fatalf("status = %s, want insufficient", report.Status)
	}
}

func TestControlTrackExplainsDivergence(t *testing.T) {
	base := Scenario{
		Name: "live", Nodes: []int{1, 2, 3, 4, 5}, Seed: 9, InitialLeader: 1,
		Actions: []Action{
			{Op: "advance", At: 0, Delay: 120},
			{Op: "client", At: 120, Node: 1, Data: "before-pause"},
			{Op: "pause", At: 120, MsgType: string(MsgAppendResp), To: 1},
			{Op: "advance", At: 120, Delay: 1 << 40},
		},
	}
	paused, ok := RunScenario(base, true)
	if !ok || !paused.Paused() {
		t.Fatal("scenario should rebuild to the pause point")
	}
	var held Message
	for _, m := range paused.PendingMessages() {
		if m.To == 1 && m.From <= 3 {
			held = m
			break
		}
	}
	if held.To != 1 {
		t.Fatal("no in-group append response to hold at the pause point")
	}
	advance := Action{Op: ActionAdvance, At: paused.Now(), Delay: 400}
	deliver := Action{
		Op: ActionDeliverFirst, At: paused.Now(),
		MsgType: string(held.Type), From: held.From, To: held.To, Term: held.Term, Data: held.Data,
	}
	docA := ExperimentDoc{ID: "a", Name: "a", Base: base, Decisions: []Decision{{
		ID: "d1", Kind: "deliver-first", Label: "deliver-first", At: paused.Now(),
		Actions: []Action{deliver, advance},
	}}}
	docB := ExperimentDoc{ID: "b", Name: "b", Base: base, Decisions: []Decision{
		{ID: "d1", Kind: "partition", Label: "partition", At: paused.Now(),
			Actions: []Action{{Op: "partition", At: paused.Now(), Groups: [][]int{{1, 2, 3}, {4, 5}}}}},
		{ID: "d2", Kind: "deliver-first", Label: "deliver-first", At: paused.Now(),
			Actions: []Action{deliver}},
		{ID: "d3", Kind: "advance", Label: "advance", At: paused.Now(), Actions: []Action{advance}},
	}}
	before, err := AnalyzeExperiments(docB, docA)
	if err != nil {
		t.Fatal(err)
	}
	if before.Status != DivergenceConfirmed {
		t.Fatalf("partitioned branch should confirm differently: %+v", before)
	}
	controlDoc, removed, err := docB.WithoutDecision("d1")
	if err != nil {
		t.Fatal(err)
	}
	if removed.Kind != "partition" || len(controlDoc.Decisions) != 2 {
		t.Fatalf("wrong decision removed: %+v", removed)
	}
	if _, err := LoadExperiment(controlDoc); err != nil {
		t.Fatalf("control track should replay without the intervention: %v", err)
	}
	after, err := AnalyzeExperiments(controlDoc, docA)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != DivergenceIdentical {
		t.Fatalf("removing the partition should make the tracks identical: %+v", after)
	}
}
