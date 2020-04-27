package sink

// causality provides a simple mechanism to improve the concurrency of SQLs execution under the premise of ensuring correctness.
// causality groups sqls that maybe contain causal relationships, and syncer executes them linearly.
// if some conflicts exist in more than one groups, then syncer waits all SQLs that are grouped be executed and reset causality.
// this mechanism meets quiescent consistency to ensure correctness.
type causality struct {
	relations map[string]int
}

func newCausality() *causality {
	return &causality{
		relations: make(map[string]int),
	}
}

func (c *causality) add(keys []string, idx int) {
	if len(keys) == 0 {
		return
	}

	for _, key := range keys {
		c.relations[key] = idx
	}
}

func (c *causality) reset() {
	c.relations = make(map[string]int)
}

// detectConflict detects whether there is a conflict
func (c *causality) detectConflict(keys []string) (bool, int) {
	if len(keys) == 0 {
		return false, 0
	}

	firstIdx := -1
	for _, key := range keys {
		if idx, ok := c.relations[key]; ok {
			if firstIdx == -1 {
				firstIdx = idx
			} else if firstIdx != idx {
				return true, -1
			}
		}
	}

	return firstIdx != -1, firstIdx
}
