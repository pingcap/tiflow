package epoch

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type SuiteTestEtcd struct {
	// Include basic suite logic.
	suite.Suite
	e         *embed.Etcd
	endpoints string
}

func allocTempURL(t *testing.T) string {
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	return fmt.Sprintf("http://127.0.0.1:%d", port)
}

func (suite *SuiteTestEtcd) SetupSuite() {
	cfg := embed.NewConfig()
	tmpDir := "suite-etcd"
	dir, err := ioutil.TempDir("", tmpDir)
	require.Nil(suite.T(), err)
	cfg.Dir = dir
	peers := allocTempURL(suite.T())
	log.Printf("Allocate server peer port is %s", peers)
	u, err := url.Parse(peers)
	require.Nil(suite.T(), err)
	cfg.LPUrls = []url.URL{*u}
	advertises := allocTempURL(suite.T())
	log.Printf("Allocate server advertises port is %s", advertises)
	u, err = url.Parse(advertises)
	require.Nil(suite.T(), err)
	cfg.LCUrls = []url.URL{*u}
	suite.e, err = embed.StartEtcd(cfg)
	if err != nil {
		require.FailNow(suite.T(), "Start embedded etcd fail:%v", err)
	}
	select {
	case <-suite.e.Server.ReadyNotify():
		log.Printf("Server is ready!")
	case <-time.After(60 * time.Second):
		suite.e.Server.Stop() // trigger a shutdown
		suite.e.Close()
		suite.e = nil
		require.FailNow(suite.T(), "Server took too long to start!")
	}
	suite.endpoints = advertises
}

// The TearDownSuite method will be run by testify once, at the very
// end of the testing suite, after all tests have been run.
func (suite *SuiteTestEtcd) TearDownSuite() {
	if suite.e != nil {
		suite.e.Server.Stop()
		suite.e.Close()
	}
}

func (suite *SuiteTestEtcd) TestEpochGenerator() {
	t := suite.T()
	genor := NewEpochGenerator(nil)
	epoch, err := genor.GenerateEpoch(context.Background())
	require.Error(t, err)
	require.Equal(t, int(0), int(epoch))

	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{
			suite.endpoints,
		},
	})
	require.Nil(t, err)
	genor = NewEpochGenerator(cli)
	testGenerator(t, genor)
	cli.Close()
}

func TestMockEpochGenerator(t *testing.T) {
	genor := NewMockEpochGenerator()
	testGenerator(t, genor)
}

func testGenerator(t *testing.T, genor Generator) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	firstEpoch, err := genor.GenerateEpoch(ctx)
	require.Nil(t, err)
	require.GreaterOrEqual(t, int(firstEpoch), int(0))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			epoch, err := genor.GenerateEpoch(ctx)
			require.Nil(t, err)
			require.GreaterOrEqual(t, int(epoch), int(0))
			oldEpoch := epoch

			epoch, err = genor.GenerateEpoch(ctx)
			require.Nil(t, err)
			require.Greater(t, int(epoch), int(oldEpoch))
		}()
	}

	wg.Wait()
	lastEpoch, err := genor.GenerateEpoch(ctx)
	require.Nil(t, err)
	require.GreaterOrEqual(t, int(lastEpoch), int(0))

	require.Equal(t, 201, int(lastEpoch)-int(firstEpoch))
}

func TestEtcdSuite(t *testing.T) {
	suite.Run(t, new(SuiteTestEtcd))
}
