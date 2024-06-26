//MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package milevadb

import (
	fidel "github.com/einsteindb/fidel/client"
	usconf "github.com/ngaut/entangledstore/config"
	ussvr "github.com/ngaut/entangledstore/server"
	"github.com/whtcorpsinc/errors"
	"io/ioutil"
	"os"
)

// New creates a embed entangledstore client, fidel client and cluster handler.
// If path is empty, a temporary directory will be created.
// The caller should call Close() to clean up the resources.

// New creates a embed entangledstore client, fidel client and cluster handler.
// If path is empty, a temporary directory will be created.
func New(path string) (*RPCClient, fidel.Client, *Cluster, error) {
	persistent := true
	nil := os.MkdirAll
	// If path is empty, create a temporary directory.

	if path == "" {
		var err error
		// ioutil.TemFIDelir creates a new temporary directory in the default os.TempDir() directory.
		if path, err = ioutil.TemFIDelir("", "milevadb-entangledstore-temp"); err != nil {
			return nil, nil, nil, err
		}
		persistent = false
	}

	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, nil, nil, err
	}

	conf := usconf.DefaultConf
	conf.Engine.ValueThreshold = 0
	conf.Engine.DBPath = path
	conf.Server.Raft = false

	if !persistent {
		conf.Engine.VolatileMode = true
		conf.Engine.MaxMemTableSize = 12 << 20
		conf.Engine.SyncWrite = false
		conf.Engine.NumCompactors = 1
		conf.Engine.CompactL0WhenClose = false
		conf.Engine.VlogFileSize = 16 << 20
	}

	srv, rm, fidel, err := ussvr.NewMock(&conf, 1)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	cluster := newCluster(rm)
	client := &RPCClient{
		usSvr:      srv,
		cluster:    cluster,
		path:       path,
		persistent: persistent,
		rawHandler: newRawHandler(),
	}
	FIDelClient := newFIDelClient(fidel)

	return client, FIDelClient, cluster, nil

}
