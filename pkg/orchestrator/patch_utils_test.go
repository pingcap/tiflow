package orchestrator

import (
	"encoding/json"
	"github.com/pingcap/check"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"sort"
)

var _ = check.Suite(&patchUtilsSuite{})

type patchUtilsSuite struct {
}

type testJsonStruct struct {
	Numbers []int
}

func generatePatch(key util.EtcdKey, i int) *DataPatch {
	return &DataPatch{
		Key: key,
		Fun: func(old []byte) ([]byte, error) {
			var s testJsonStruct
			if old != nil {
				err := json.Unmarshal(old, &s)
				if err != nil {
					return nil, err
				}
			}
			s.Numbers = append(s.Numbers, i)
			return json.Marshal(&s)
		},
	}
}

func generateIgnorePatch(key util.EtcdKey) *DataPatch {
	return &DataPatch{
		Key: key,
		Fun: func(old []byte) ([]byte, error) {
			return nil, cerrors.ErrEtcdIgnore.GenWithStackByArgs()
		},
	}
}


func (s *patchUtilsSuite) TestMergeCommutativePatchesBasic(c *check.C) {
	defer testleak.AfterTest(c)()
	patches := make([]*DataPatch, 0, 20)
	for i := 0; i < 10; i++ {
		patches = append(patches, generatePatch(util.NewEtcdKey("/test_1"), i))
		patches = append(patches, generatePatch(util.NewEtcdKey("/test_2"), i))
	}

	mergedPatches := MergeCommutativePatches(patches)
	c.Assert(mergedPatches, check.HasLen, 2)

	var (
		bytes1 []byte
		bytes2 []byte
	)

	for i := 0; i < 2; i++ {
		patch := mergedPatches[i]
		if patch.Key == util.NewEtcdKey("/test_1") {
			var err error
			bytes1, err = patch.Fun(bytes1)
			c.Assert(err, check.IsNil)
			continue
		} else if patch.Key == util.NewEtcdKey("/test_2") {
			var err error
			bytes2, err = patch.Fun(bytes2)
			c.Assert(err, check.IsNil)
			continue
		}
		c.Fail()
	}

	var data testJsonStruct
	err := json.Unmarshal(bytes1, &data)
	c.Assert(err, check.IsNil)
	sort.Ints(data.Numbers)
	c.Assert(data.Numbers, check.DeepEquals, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	err = json.Unmarshal(bytes2, &data)
	c.Assert(err, check.IsNil)
	sort.Ints(data.Numbers)
	c.Assert(data.Numbers, check.DeepEquals, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
}
