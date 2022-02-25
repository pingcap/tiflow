package verification

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/leakutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
	}
	leakutil.SetUpLeakTest(m, opts...)
}

func TestNewVerification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	origin := openDB
	defer func() {
		openDB = origin
	}()

	openDB = func(ctx context.Context, dsn string) (*sql.DB, error) {
		db, _, err := sqlmock.New()
		return db, err
	}
	err := NewVerification(ctx, &Config{CheckInterval: time.Minute})
	require.Nil(t, err)

	openDB = func(ctx context.Context, dsn string) (*sql.DB, error) {
		db, _, _ := sqlmock.New()
		return db, errors.New("openDB err")
	}
	err = NewVerification(ctx, &Config{})
	require.EqualError(t, err, "openDB err")

	db, _, err := sqlmock.New()
	require.Nil(t, err)
	v := &TiDBVerification{
		config: &Config{
			CheckInterval: 10 * time.Millisecond,
		},
		running:           *atomic.NewBool(true),
		upstreamChecker:   newChecker(db),
		downstreamChecker: newChecker(db),
	}
	ctx, cancel = context.WithTimeout(ctx, time.Millisecond*30)
	defer cancel()
	require.NotPanics(t, func() {
		v.runVerify(ctx)
	})
}

func TestCheckConsistency(t *testing.T) {
	type args struct {
		ts tsPair
	}
	tests := []struct {
		name         string
		args         args
		wantErr      error
		wantCheckErr error
		wantRet      bool
	}{
		{
			name:    "happy",
			args:    args{tsPair{primaryTs: "1", secondaryTs: "2", cf: "happy"}},
			wantRet: true,
		},
		{
			name:    "setSnapshot err",
			args:    args{tsPair{primaryTs: "1", secondaryTs: "2", cf: "setSnapshot err"}},
			wantErr: errors.New("setSnapshot"),
		},
		{
			name:         "compareCheckSum err",
			args:         args{tsPair{primaryTs: "1", secondaryTs: "2", cf: "compareCheckSum err"}},
			wantCheckErr: errors.New("compareCheckSum"),
		},
	}

	for _, tt := range tests {
		db, mockDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mockDB.ExpectExec(fmt.Sprintf(`set @@tidb_snapshot=%s`, tt.args.ts.primaryTs)).WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(tt.wantErr)
		mockDB.ExpectExec(fmt.Sprintf(`set @@tidb_snapshot=%s`, tt.args.ts.secondaryTs)).WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(tt.wantErr)
		mockDB.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{})).WillReturnError(tt.wantCheckErr)

		v := &TiDBVerification{
			upstreamChecker:   newChecker(db),
			downstreamChecker: newChecker(db),
			config: &Config{
				Filter: &filter.Filter{},
			},
		}
		ret, err := v.checkConsistency(context.Background(), tt.args.ts)
		require.Equal(t, tt.wantRet, ret, tt.name)
		if tt.wantErr != nil {
			require.True(t, errors.ErrorEqual(tt.wantErr, err), tt.name)
		}
		if tt.wantCheckErr != nil {
			require.True(t, errors.ErrorEqual(tt.wantCheckErr, err), tt.name)
		}
		db.Close()
	}
}

func TestUpdateCheckResult(t *testing.T) {
	type args struct {
		ts       tsPair
		checkRet int
	}

	tests := []struct {
		name          string
		args          args
		wantSnapErr   error
		wantTxErr     error
		wantExErr     error
		wantCommitErr error
	}{
		{
			name: "happy",
			args: args{ts: tsPair{}, checkRet: checkPass},
		},
		{
			name:        "snapshot err",
			args:        args{ts: tsPair{}, checkRet: checkPass},
			wantSnapErr: errors.New("snapshot err"),
		},
		{
			name:      "tx err",
			args:      args{ts: tsPair{}, checkRet: checkPass},
			wantTxErr: errors.New("tx err"),
		},
		{
			name:      "exe err",
			args:      args{ts: tsPair{}, checkRet: checkPass},
			wantExErr: errors.New("exe err"),
		},
		{
			name:          "commit err",
			args:          args{ts: tsPair{}, checkRet: checkPass},
			wantCommitErr: errors.New("commit err"),
		},
	}
	for _, tt := range tests {
		db, mockDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mockDB.ExpectExec(fmt.Sprintf(`set @@tidb_snapshot=%s`, "0")).WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(tt.wantSnapErr)
		mockDB.ExpectBegin().WillReturnError(tt.wantTxErr)
		mockDB.ExpectExec(fmt.Sprintf("update %s.%s set result=? where primary_ts=? and secondary_ts=? and cf=?", "db", "test")).WithArgs(tt.args.checkRet, tt.args.ts.primaryTs, tt.args.ts.secondaryTs, tt.args.ts.cf).WillReturnError(tt.wantExErr).WillReturnResult(sqlmock.NewResult(1, 1))
		if tt.wantExErr != nil {
			mockDB.ExpectRollback().WillReturnError(errors.New("who cares"))
		}
		mockDB.ExpectCommit().WillReturnError(tt.wantCommitErr)
		v := &TiDBVerification{
			downstreamChecker: newChecker(db),
			config: &Config{
				DataBaseName: "db",
				TableName:    "test",
			},
		}
		err = v.updateCheckResult(context.Background(), tt.args.ts, tt.args.checkRet)
		if tt.wantSnapErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantSnapErr), tt.name)
		} else if tt.wantExErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantExErr), tt.name)
		} else if tt.wantTxErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantTxErr), tt.name)
		} else if tt.wantCommitErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantCommitErr), tt.name)
		} else {
			require.Nil(t, err, tt.name)
		}
	}
}

func TestGetTs(t *testing.T) {
	type args struct {
		ts tsPair
	}
	tests := []struct {
		name    string
		args    args
		wantTs  tsPair
		wantErr error
	}{
		{
			name:   "happy",
			args:   args{ts: tsPair{primaryTs: "p", secondaryTs: "s", cf: "c"}},
			wantTs: tsPair{primaryTs: "p", secondaryTs: "s", cf: "c"},
		},
		{
			name:    "err",
			args:    args{ts: tsPair{primaryTs: "p", secondaryTs: "s", cf: "c"}},
			wantErr: errors.New("err"),
		},
	}
	for _, tt := range tests {
		db, mockDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mockDB.ExpectQuery(fmt.Sprintf("select max(primary_ts) as primary_ts from %s.%s where cf=?", "db", "test")).WithArgs(tt.args.ts.cf).WillReturnError(tt.wantErr).WillReturnRows(sqlmock.NewRows([]string{"primary_ts"}).AddRow(tt.args.ts.primaryTs))

		mockDB.ExpectQuery(fmt.Sprintf("select cf, primary_ts, secondary_ts, result from %s.%s where cf=? and primary_ts=?", "db", "test")).WithArgs(tt.args.ts.cf, tt.args.ts.primaryTs).WillReturnError(tt.wantErr).WillReturnRows(sqlmock.NewRows([]string{"cf", "primary_ts", "secondary_ts", "result"}).AddRow(tt.args.ts.cf, tt.args.ts.primaryTs, tt.args.ts.secondaryTs, tt.args.ts.result))

		v := &TiDBVerification{
			downstreamChecker: newChecker(db),
			config: &Config{
				DataBaseName: "db",
				TableName:    "test",
				ChangefeedID: tt.args.ts.cf,
			},
		}
		ret, err := v.getTS(context.Background())
		require.True(t, errors.ErrorEqual(tt.wantErr, err), tt.name)
		require.Equal(t, tt.wantTs, ret, tt.name)
		db.Close()
	}
}

func TestGetPreviousTs(t *testing.T) {
	type args struct {
		ts tsPair
	}
	tests := []struct {
		name    string
		args    args
		wantTs  tsPair
		wantErr error
	}{
		{
			name:   "happy",
			args:   args{ts: tsPair{primaryTs: "1", secondaryTs: "s", cf: "c"}},
			wantTs: tsPair{primaryTs: "1", secondaryTs: "s", cf: "c"},
		},
		{
			name:    "err",
			args:    args{ts: tsPair{primaryTs: "1", secondaryTs: "s", cf: "c"}},
			wantErr: errors.New("err"),
		},
	}
	for _, tt := range tests {
		db, mockDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)

		mockDB.ExpectQuery(fmt.Sprintf("select cf, primary_ts, secondary_ts, result from %s.%s where cf=? and primary_ts<? order by primary_ts desc limit 1", "db", "test")).WithArgs(tt.args.ts.cf, 1).WillReturnError(tt.wantErr).WillReturnRows(sqlmock.NewRows([]string{"cf", "primary_ts", "secondary_ts", "result"}).AddRow(tt.args.ts.cf, tt.args.ts.primaryTs, tt.args.ts.secondaryTs, tt.args.ts.result))

		v := &TiDBVerification{
			downstreamChecker: newChecker(db),
			config: &Config{
				DataBaseName: "db",
				TableName:    "test",
				ChangefeedID: tt.args.ts.cf,
			},
		}
		ret, err := v.getPreviousTS(context.Background(), tt.args.ts.cf, tt.args.ts.primaryTs)
		require.True(t, errors.ErrorEqual(tt.wantErr, err), tt.name)
		require.Equal(t, tt.wantTs, ret, tt.name)
		db.Close()
	}
}

func TestVerify(t *testing.T) {
	type args struct {
		ts       tsPair
		pts      []tsPair
		checkRet []int
	}
	tests := []struct {
		name          string
		args          args
		wantGetTsErr  error
		wantCheckErr  error
		wantUpdateErr error
		wantGetPreErr error
		wantSts       string
		wantEts       string
	}{
		{
			name: "happy already pass",
			args: args{
				ts: tsPair{
					result: checkPass,
				},
				pts: []tsPair{
					{
						result: 11,
					},
				},
				checkRet: []int{111},
			},
		},
		{
			name: "happy already fail",
			args: args{
				ts: tsPair{
					result: checkFail,
				},
				pts: []tsPair{
					{
						result: 11,
					},
				},
				checkRet: []int{111},
			},
		},
		{
			name: "happy first uncheck, then pass",
			args: args{
				ts: tsPair{
					result: unchecked,
				},
				pts: []tsPair{
					{
						result: 11,
					},
				},
				checkRet: []int{checkPass},
			},
		},
		{
			name: "happy first uncheck, then fail, first pre pass",
			args: args{
				ts: tsPair{
					primaryTs: "11",
					result:    unchecked,
				},
				pts: []tsPair{
					{
						primaryTs: "1",
						result:    checkPass,
					},
				},
				checkRet: []int{checkFail},
			},
			wantSts: "1",
			wantEts: "11",
		},
		{
			name: "happy first uncheck, then fail, first pre fail",
			args: args{
				ts: tsPair{
					primaryTs: "11",
					result:    unchecked,
				},
				pts: []tsPair{
					{
						primaryTs: "1",
						result:    checkFail,
					},
				},
				checkRet: []int{checkFail},
			},
		},
		{
			name: "happy first uncheck, then fail, first pre uncheck, then pass",
			args: args{
				ts: tsPair{
					primaryTs: "11",
					result:    unchecked,
				},
				pts: []tsPair{
					{
						primaryTs:   "2",
						secondaryTs: "221",
						result:      unchecked,
					},
				},
				checkRet: []int{checkFail, checkPass},
			},
			wantSts: "2",
			wantEts: "11",
		},
	}
	for _, tt := range tests {
		upDB, mockUpDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		downDB, mockDownDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)

		mockDownDB.ExpectQuery(fmt.Sprintf("select max(primary_ts) as primary_ts from %s.%s where cf=?", "db", "test")).WithArgs(tt.args.ts.cf).WillReturnError(tt.wantGetTsErr).WillReturnRows(sqlmock.NewRows([]string{"primary_ts"}).AddRow(tt.args.ts.primaryTs))
		mockDownDB.ExpectQuery(fmt.Sprintf("select cf, primary_ts, secondary_ts, result from %s.%s where cf=? and primary_ts=?", "db", "test")).WithArgs(tt.args.ts.cf, tt.args.ts.primaryTs).WillReturnError(tt.wantGetTsErr).WillReturnRows(sqlmock.NewRows([]string{"cf", "primary_ts", "secondary_ts", "result"}).AddRow(tt.args.ts.cf, tt.args.ts.primaryTs, tt.args.ts.secondaryTs, tt.args.ts.result))

		mockUpDB.ExpectExec(fmt.Sprintf(`set @@tidb_snapshot=%s`, tt.args.ts.primaryTs)).WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(tt.wantCheckErr)
		mockDownDB.ExpectExec(fmt.Sprintf(`set @@tidb_snapshot=%s`, tt.args.ts.secondaryTs)).WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(tt.wantCheckErr)

		origin := compareCheckSum
		i := 0
		compareCheckSum = func(ctx context.Context, upstreamChecker, downstreamChecker checkSumChecker, f *filter.Filter) (bool, error) {
			ret := false
			ret = tt.args.checkRet[i] == checkPass
			i++
			return ret, tt.wantCheckErr
		}

		mockDownDB.ExpectExec(fmt.Sprintf(`set @@tidb_snapshot=%s`, "0")).WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(tt.wantUpdateErr)
		mockDownDB.ExpectBegin().WillReturnError(tt.wantUpdateErr)
		mockDownDB.ExpectExec(fmt.Sprintf("update %s.%s set result=? where primary_ts=? and secondary_ts=? and cf=?", "db", "test")).WithArgs(tt.args.checkRet[0], tt.args.ts.primaryTs, tt.args.ts.secondaryTs, tt.args.ts.cf).WillReturnError(tt.wantUpdateErr).WillReturnResult(sqlmock.NewResult(1, 1))

		if tt.wantUpdateErr != nil {
			mockDownDB.ExpectRollback().WillReturnError(errors.New("who cares"))
		}
		mockDownDB.ExpectCommit().WillReturnError(nil)

		mockDownDB.ExpectQuery(fmt.Sprintf("select cf, primary_ts, secondary_ts, result from %s.%s where cf=? and primary_ts<? order by primary_ts desc limit 1", "db", "test")).WithArgs(tt.args.ts.cf, 11).WillReturnError(tt.wantGetPreErr).WillReturnRows(sqlmock.NewRows([]string{"cf", "primary_ts", "secondary_ts", "result"}).AddRow(tt.args.pts[0].cf, tt.args.pts[0].primaryTs, tt.args.pts[0].secondaryTs, tt.args.pts[0].result))

		if len(tt.args.checkRet) > 1 {
			mockUpDB.ExpectExec(fmt.Sprintf(`set @@tidb_snapshot=%s`, tt.args.pts[0].primaryTs)).WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(tt.wantCheckErr)
			mockDownDB.ExpectExec(fmt.Sprintf(`set @@tidb_snapshot=%s`, tt.args.pts[0].secondaryTs)).WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(tt.wantCheckErr)

			mockDownDB.ExpectExec(fmt.Sprintf(`set @@tidb_snapshot=%s`, "0")).WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(tt.wantUpdateErr)
			mockDownDB.ExpectBegin().WillReturnError(tt.wantUpdateErr)
			mockDownDB.ExpectExec(fmt.Sprintf("update %s.%s set result=? where primary_ts=? and secondary_ts=? and cf=?", "db", "test")).WithArgs(tt.args.checkRet[1], tt.args.pts[0].primaryTs, tt.args.pts[0].secondaryTs, tt.args.pts[0].cf).WillReturnError(tt.wantUpdateErr).WillReturnResult(sqlmock.NewResult(1, 1))
			mockDownDB.ExpectCommit().WillReturnError(nil)
		}
		v := &TiDBVerification{
			upstreamChecker:   newChecker(upDB),
			downstreamChecker: newChecker(downDB),
			config: &Config{
				DataBaseName: "db",
				TableName:    "test",
				ChangefeedID: tt.args.ts.cf,
			},
		}
		sts, ets, err := v.Verify(context.Background())
		if tt.wantGetTsErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantGetTsErr), tt.name)
		} else if tt.wantCheckErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantCheckErr), tt.name)
		} else if tt.wantGetPreErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantGetPreErr), tt.name)
		} else {
			require.Nil(t, err, tt.name)
		}
		require.Equal(t, tt.wantSts, sts, tt.name)
		require.Equal(t, tt.wantEts, ets, tt.name)

		compareCheckSum = origin
	}
}
