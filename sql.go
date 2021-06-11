package edb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/mirror"
)

type SQLDBSyncer struct {
	Table       string
	NoOverwrite bool

	Syncer mirror.Syncer
	DB     *sql.DB
}

func (ds *SQLDBSyncer) Validate() error {
	// TODO: validate table schema
	return nil
}

func (ds *SQLDBSyncer) Sync(ctx context.Context) error {
	var (
		stmt  *sql.Stmt
		dstmt *sql.Stmt

		err error
	)

	if ds.NoOverwrite {
		stmt, err = ds.DB.Prepare(fmt.Sprintf("INSERT INTO %s(key,event,value,revision) VALUES ($1,$2,$3,$4) ON CONFLICT(revision) DO NOTHING", ds.Table))
		if err != nil {
			return err
		}
		dstmt = stmt
	} else {
		stmt, err = ds.DB.Prepare(fmt.Sprintf("INSERT INTO %s(key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2", ds.Table))
		if err != nil {
			return err
		}

		dstmt, err = ds.DB.Prepare(fmt.Sprintf("DELETE FROM %s WHERE value=$1", ds.Table))
		if err != nil {
			return err
		}
	}

	rc, errc := ds.Syncer.SyncBase(ctx)
FOR:
	for {
		select {
		case r, ok := <-rc:
			// ok == false means that buffered channel rc is closed;
			// if rc is closed and len(r.Kvs) > 0, do process;
			// if rc is closed and len(r.Kvs) == 0, break for loop because channel is empty and closed;
			// if rc is open, do process. maybe len(r.Kvs) == 0.
			if !ok && len(r.Kvs) == 0 {
				break FOR
			}
			for _, kv := range r.Kvs {
				if ds.NoOverwrite {
					if _, err := stmt.Exec(kv.Key, clientv3.EventTypePut.String(), kv.Value, kv.ModRevision); err != nil {
						return err
					}
					continue
				}
				if _, err := stmt.Exec(kv.Key, kv.Value); err != nil {
					return err
				}
			}
		case e := <-errc:
			if e != nil {
				return e
			}
		}
	}

	wch := ds.Syncer.SyncUpdates(ctx)
	for wr := range wch {
		if wr.Err() != nil {
			return wr.Err()
		}

		for _, e := range wr.Events {
			if ds.NoOverwrite {
				if _, err := stmt.Exec(e.Kv.Key, e.Type.String(), e.Kv.Value, e.Kv.ModRevision); err != nil {
					return err
				}
				continue
			}

			switch e.Type {
			case clientv3.EventTypePut:
				if _, err := stmt.Exec(e.Kv.Key, e.Kv.Value); err != nil {
					return err
				}
			case clientv3.EventTypeDelete:
				if _, err := dstmt.Exec(e.Kv.Key); err != nil {
					return err
				}
			}
		}
	}

	return ctx.Err()
}
