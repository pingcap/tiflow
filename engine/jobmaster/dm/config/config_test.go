package config

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/stretchr/testify/require"
)

const (
	jobTemplatePath    = "./job_template.yaml"
	subtaskTemplateDir = "."
)

func TestJobCfg(t *testing.T) {
	t.Parallel()

	jobCfg := &JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))
	require.Equal(t, "test", jobCfg.Name)
	content, err := jobCfg.Yaml()
	require.NoError(t, err)
	jobCfg2 := &JobCfg{}
	require.NoError(t, jobCfg2.Decode([]byte(content)))
	require.Equal(t, jobCfg, jobCfg2)

	clone, err := jobCfg.Clone()
	require.NoError(t, err)
	require.EqualValues(t, clone, jobCfg)

	require.Error(t, jobCfg.DecodeFile("./job_not_exist.yaml"))
}

func TestTaskCfg(t *testing.T) {
	t.Parallel()
	jobCfg := &JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))

	taskCfgs := jobCfg.ToTaskConfigs()
	for _, taskCfg := range taskCfgs {
		subTaskCfg := taskCfg.ToDMSubTaskCfg()
		expectCfg := &dmconfig.SubTaskConfig{}
		_, err := toml.DecodeFile(fmt.Sprintf("%s/dm_subtask_%d.toml", subtaskTemplateDir, taskCfg.Upstreams[0].DBCfg.Port), expectCfg)
		require.NoError(t, err)
		require.EqualValues(t, subTaskCfg, expectCfg)
	}
}
