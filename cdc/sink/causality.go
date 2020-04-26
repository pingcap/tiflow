package sink

// causality provides a simple mechanism to improve the concurrency of SQLs execution under the premise of ensuring correctness.
// causality groups sqls that maybe contain causal relationships, and syncer executes them linearly.
// if some conflicts exist in more than one groups, then syncer waits all SQLs that are grouped be executed and reset causality.
// this mechanism meets quiescent consistency to ensure correctness.
type causality struct {
	relations map[string]struct{}
}

func newCausality() *causality {
	return &causality{
		relations: make(map[string]struct{}),
	}
}

func (c *causality) add(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	for _, key := range keys {
		c.relations[key] = struct{}{}
	}
	return nil
}

func (c *causality) reset() {
	c.relations = make(map[string]struct{})
}

// detectConflict detects whether there is a conflict
func (c *causality) detectConflict(keys []string) bool {
	if len(keys) == 0 {
		return false
	}

	for _, key := range keys {
		if _, ok := c.relations[key]; ok {
			return true
		}
	}

	return false
}
