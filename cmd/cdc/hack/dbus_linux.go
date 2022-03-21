// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux
// +build linux

package hack

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	dbusSessionEnvName    = "DBUS_SESSION_BUS_ADDRESS"
	dbusSessionBusPidItem = "DBUS_SESSION_BUS_PID"
)

var (
	execCommand = exec.Command
	pid         int
	once        sync.Once
)

// init check DBUS_SESSION_BUS_ADDRESS first and then try to discovery it.
// if DBUS_SESSION_BUS_ADDRESS is found, do nothing, go-dbus will not create daemon-dbus,
// if not, then create daemon-dbus, and set DBUS_SESSION_BUS_ADDRESS env and record the pid
// so can we kill the created process when cdc command is stopped
func init() {
	if address := os.Getenv(dbusSessionEnvName); address != "" && address != "autolaunch:" {
		return
	} else if canDiscoverDbusSessionBusAddress() {
		return
	}

	cmd := execCommand("dbus-launch")
	b, err := cmd.CombinedOutput()
	if err != nil {
		return
	}

	// the output is something like
	// DBUS_SESSION_BUS_ADDRESS=unix:abstract=/tmp/dbus-18tPZg6i5m,guid=aae98bb29cd89d5f6d98d8cc6232d272
	// DBUS_SESSION_BUS_PID=2520491
	firstIndex := bytes.IndexByte(b, '=')
	lastIndex := bytes.IndexByte(b, '\n')

	if firstIndex == -1 || lastIndex == -1 || firstIndex > lastIndex {
		return
	}

	env, addr := string(b[0:firstIndex]), string(b[firstIndex+1:lastIndex])
	// set DBUS_SESSION_BUS_ADDRESS env to avoid godbus create a new daemon-dubs process
	os.Setenv(env, addr)
	firstIndex = bytes.Index(b, []byte(dbusSessionBusPidItem))
	if firstIndex == -1 {
		log.Warn("can not parse daemon-dbus process id", zap.String("output", string(b)))
		return
	}
	pid, _ = strconv.Atoi(strings.TrimSpace(string(b[firstIndex+len(dbusSessionBusPidItem)+1:])))
	log.Info("daemon-dbus is started", zap.Int("pid", pid))
}

// TryClearDbusDaemon kill the created daemon-dbus process
func TryClearDbusDaemon() {
	once.Do(func() {
		if pid > 0 {
			proc, err := os.FindProcess(pid)
			if err == nil {
				// Kill the process
				log.Info("killing daemon-dbus process", zap.Int("pid", pid))
				_ = proc.Kill()
			}
		}
	})
}

// canDiscoverDbusSessionBusAddress check if we can discover an existing dbus session
// and return the value of its DBUS_SESSION_BUS_ADDRESS.
// It tries different techniques employed by different operating systems,
// returning the first valid address it finds, or an empty string.
//
// * /run/user/<uid>/bus           if this exists, it *is* the bus socket. present on
//                                 Ubuntu 18.04
// * /run/user/<uid>/dbus-session: if this exists, it can be parsed for the bus
//                                 address. present on Ubuntu 16.04
//
// See https://dbus.freedesktop.org/doc/dbus-launch.1.html
func canDiscoverDbusSessionBusAddress() bool {
	if runtimeDirectory, err := getRuntimeDirectory(); err == nil {

		if runUserBusFile := path.Join(runtimeDirectory, "bus"); fileExists(runUserBusFile) {
			// if /run/user/<uid>/bus exists, that file itself
			// *is* the unix socket, so return its path
			return true
		}
		if runUserSessionDbusFile := path.Join(runtimeDirectory, "dbus-session"); fileExists(runUserSessionDbusFile) {
			// if /run/user/<uid>/dbus-session exists, it's a
			// text file // containing the address of the socket, e.g.:
			// DBUS_SESSION_BUS_ADDRESS=unix:abstract=/tmp/dbus-E1c73yNqrG

			if f, err := ioutil.ReadFile(runUserSessionDbusFile); err == nil {
				fileContent := string(f)

				prefix := "DBUS_SESSION_BUS_ADDRESS="

				if strings.HasPrefix(fileContent, prefix) {
					address := strings.TrimRight(strings.TrimPrefix(fileContent, prefix), "\n\r")
					return address != ""
				}
			}
		}
	}
	return false
}

func getRuntimeDirectory() (string, error) {
	var (
		currentUser *user.User
		err         error
	)
	if currentUser, err = user.Current(); err != nil {
		return "", err
	}
	return fmt.Sprintf("/run/user/%s", currentUser.Uid), nil
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}
