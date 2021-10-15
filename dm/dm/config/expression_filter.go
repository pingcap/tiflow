package config

// TODO: move related struct to tidb-tools

// ExpressionFilter represents a filter that will be applied on row changes.
// one ExpressionFilter can only have one of (insert, update, delete) expressions.
// there are two update expressions, which form an AND logic. If user omits one expression, DM will use "TRUE" for it.
type ExpressionFilter struct {
	Schema             string `yaml:"schema" toml:"schema" json:"schema"`
	Table              string `yaml:"table" toml:"table" json:"table"`
	InsertValueExpr    string `yaml:"insert-value-expr" toml:"insert-value-expr" json:"insert-value-expr"`
	UpdateOldValueExpr string `yaml:"update-old-value-expr" toml:"update-old-value-expr" json:"update-old-value-expr"`
	UpdateNewValueExpr string `yaml:"update-new-value-expr" toml:"update-new-value-expr" json:"update-new-value-expr"`
	DeleteValueExpr    string `yaml:"delete-value-expr" toml:"delete-value-expr" json:"delete-value-expr"`
}
