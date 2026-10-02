package raft

import "fmt"

// Delivery is one observed message arrival on an experiment track.
type Delivery struct {
	At   int64       `json:"at"`
	Type MessageType `json:"type"`
	From int         `json:"from"`
	To   int         `json:"to"`
	Term int         `json:"term"`
	Data string      `json:"data,omitempty"`
}

// Confirmation is one commit decision a node recorded on a track.
type Confirmation struct {
	At    int64  `json:"at"`
	Node  int    `json:"node"`
	Index int    `json:"index"`
	Term  int    `json:"term"`
	Data  string `json:"data"`
}

// Track summarizes the observable behaviour of one experiment so two
// tracks can be compared without sharing simulator internals.
type Track struct {
	Deliveries    []Delivery     `json:"deliveries"`
	Confirmations []Confirmation `json:"confirmations"`
}

// Divergence categories for the first decision point where two tracks part.
const (
	DivergenceConfirmed    = "confirmed"    // nodes already confirmed differently
	DivergenceOrderOnly    = "order-only"   // only message arrival order differs
	DivergenceInsufficient = "insufficient" // not enough recorded data yet
	DivergenceIdentical    = "identical"    // no divergence observed so far
)

// DivergenceReport pinpoints the first divergence between two tracks and
// carries anchors (time, node, log index, message) so the UI can jump back
// to the exact message and node records behind the verdict.
type DivergenceReport struct {
	Status    string    `json:"status"`
	Detail    string    `json:"detail"`
	At        int64     `json:"at,omitempty"`
	Node      int       `json:"node,omitempty"`
	Index     int       `json:"index,omitempty"`
	DataA     string    `json:"dataA,omitempty"`
	DataB     string    `json:"dataB,omitempty"`
	DeliveryA *Delivery `json:"deliveryA,omitempty"`
	DeliveryB *Delivery `json:"deliveryB,omitempty"`
}

// SummarizeEvents reduces a raw event stream to its delivery and
// confirmation sequences, preserving order.
func SummarizeEvents(events []Event) Track {
	track := Track{Deliveries: []Delivery{}, Confirmations: []Confirmation{}}
	for _, e := range events {
		switch e.Kind {
		case "receive":
			if e.Message == nil || e.Message.Type == MsgClient {
				continue
			}
			m := e.Message
			track.Deliveries = append(track.Deliveries, Delivery{
				At: e.VirtualTime, Type: m.Type, From: m.From, To: m.To, Term: m.Term, Data: m.Data,
			})
		case "commit":
			track.Confirmations = append(track.Confirmations, Confirmation{
				At: e.VirtualTime, Node: e.Node, Index: e.Index, Term: e.Term, Data: e.Data,
			})
		}
	}
	return track
}

// CompareTracks finds the first point where two tracks diverge. Confirmed
// decisions outrank pure ordering differences: if nodes already committed
// different records, that is the divergence that matters.
func CompareTracks(a, b Track) DivergenceReport {
	shared := len(a.Confirmations)
	if len(b.Confirmations) < shared {
		shared = len(b.Confirmations)
	}
	for i := 0; i < shared; i++ {
		ca, cb := a.Confirmations[i], b.Confirmations[i]
		if ca != cb {
			return DivergenceReport{
				Status: DivergenceConfirmed,
				Detail: fmt.Sprintf("节点 %d 的第 %d 条确认不同：%q 对 %q", ca.Node, ca.Index, ca.Data, cb.Data),
				At:     ca.At, Node: ca.Node, Index: ca.Index, DataA: ca.Data, DataB: cb.Data,
			}
		}
	}
	if len(a.Confirmations) != len(b.Confirmations) {
		extra := a.Confirmations
		aLonger := true
		if len(b.Confirmations) > shared {
			extra = b.Confirmations
			aLonger = false
		}
		missing := extra[shared]
		report := DivergenceReport{
			Status: DivergenceConfirmed,
			At:     missing.At, Node: missing.Node, Index: missing.Index,
			Detail: fmt.Sprintf("节点 %d 在一条轨迹上确认了 #%d %q，另一条尚未确认", missing.Node, missing.Index, missing.Data),
		}
		if aLonger {
			report.DataA = missing.Data
		} else {
			report.DataB = missing.Data
		}
		return report
	}
	sharedDeliveries := len(a.Deliveries)
	if len(b.Deliveries) < sharedDeliveries {
		sharedDeliveries = len(b.Deliveries)
	}
	for i := 0; i < sharedDeliveries; i++ {
		da, db := a.Deliveries[i], b.Deliveries[i]
		if da != db {
			daCopy, dbCopy := da, db
			return DivergenceReport{
				Status: DivergenceOrderOnly,
				Detail: fmt.Sprintf("第 %d 次投递不同：%s %d→%d 对 %s %d→%d（确认结果仍一致）",
					i+1, da.Type, da.From, da.To, db.Type, db.From, db.To),
				At: da.At, DeliveryA: &daCopy, DeliveryB: &dbCopy,
			}
		}
	}
	if len(a.Deliveries) != len(b.Deliveries) {
		extra := a.Deliveries
		if len(b.Deliveries) > sharedDeliveries {
			extra = b.Deliveries
		}
		missing := extra[sharedDeliveries]
		missingCopy := missing
		report := DivergenceReport{
			Status: DivergenceOrderOnly,
			Detail: fmt.Sprintf("一条轨迹多投递了 %s %d→%d（确认结果仍一致）", missing.Type, missing.From, missing.To),
			At:     missing.At,
		}
		if len(a.Deliveries) > sharedDeliveries {
			report.DeliveryA = &missingCopy
		} else {
			report.DeliveryB = &missingCopy
		}
		return report
	}
	return DivergenceReport{Status: DivergenceIdentical, Detail: "两条轨迹的投递顺序与确认记录完全一致"}
}

// AnalyzeExperiments compares two experiment tracks. A track with no
// recorded post-fork decisions cannot support a verdict, so the report
// says the data is incomplete instead of guessing.
func AnalyzeExperiments(docA, docB ExperimentDoc) (DivergenceReport, error) {
	if len(docA.Decisions) == 0 || len(docB.Decisions) == 0 {
		return DivergenceReport{
			Status: DivergenceInsufficient,
			Detail: "至少一条轨迹还没有分叉后的决定记录，资料尚未完整",
		}, nil
	}
	simA, err := LoadExperiment(docA)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("实验 %s 无法重放：%v", docA.ID, err)
	}
	simB, err := LoadExperiment(docB)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("实验 %s 无法重放：%v", docB.ID, err)
	}
	return CompareTracks(SummarizeEvents(simA.Events()), SummarizeEvents(simB.Events())), nil
}
