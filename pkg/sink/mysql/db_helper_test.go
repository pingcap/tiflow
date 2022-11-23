package mysql

import (
	"encoding/base64"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodePassword(t *testing.T) {
	t.Parallel()

	type args struct {
		password string
	}
	tests := []struct {
		name       string
		password   string
		want       string
		needEncode bool
		needEscape bool
	}{
		{
			name:     "case1",
			password: "123456",
			want:     "123456",
		},
		{
			name:       "case2",
			password:   "asdeer112",
			want:       "asdeer112",
			needEncode: true,
		},
		{
			name:       "case3",
			password:   "asdeer112!@#&",
			want:       "asdeer112!@#&",
			needEscape: true,
		},
		{
			name:       "case4",
			password:   "!@#12312//",
			want:       "!@#12312//",
			needEncode: true,
			needEscape: true,
		},
	}
	for _, c := range tests {
		var err error
		if c.needEscape {
			c.password = url.QueryEscape(c.password)
		}
		if c.needEncode {
			c.password = base64.StdEncoding.EncodeToString([]byte(c.password))
			tem, err := base64.StdEncoding.DecodeString(c.password)
			c.password = string(tem)
			require.NoError(t, err, c.name)
		}
		if c.needEscape {
			c.password, err = url.QueryUnescape(c.password)
			require.NoError(t, err, c.name)
		}
		require.Equal(t, c.want, string(c.password))
	}
}
