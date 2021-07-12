package version

import (
	"fmt"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/spf13/cobra"
)

func NewCmdVersion() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Output version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version.GetRawInfo())
		},
	}
}
