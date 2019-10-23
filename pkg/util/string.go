package util

import (
	"fmt"
	"strings"
)

func QuoteSchema(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", EscapeName(schema), EscapeName(table))
}

func QuoteName(name string) string {
	return "`" + EscapeName(name) + "`"
}

func EscapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

func HolderString(n int) string {
	var builder strings.Builder
	builder.Grow((n-1)*2 + 1)
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	return builder.String()
}
