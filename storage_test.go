package certmagicsql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"log"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/caddyserver/certmagic"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func setup(ctx context.Context, t *testing.T) *postgresStorage {
	t.Helper()
	return setupWithOptions(ctx, t, Options{})
}

func setupWithOptions(ctx context.Context, t *testing.T, options Options) *postgresStorage {
	t.Helper()

	connStr := os.Getenv("CONN_STR")
	if connStr == "" {
		t.Skipf("must set CONN_STR")
	}
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := NewStorage(ctx, db, options)
	if err != nil {
		t.Fatal(err)
	}
	return storage.(*postgresStorage)
}

func dropTable() {
	connStr := os.Getenv("CONN_STR")
	if connStr == "" {
		log.Println("must set CONN_STR")
		return
	}
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("drop table if exists certmagic_data")
	if err != nil {
		log.Fatal(err)
	}
}

func TestMain(m *testing.M) {
	dropTable()
	os.Exit(m.Run())
}

func TestExists(t *testing.T) {
	var err error

	ctx := context.Background()
	storage := setup(ctx, t)

	key := "testkey"
	defer storage.Delete(ctx, key)

	exists := storage.Exists(ctx, key)
	if exists {
		t.Fatalf("key should not exist")
	}
	err = storage.Store(ctx, key, []byte("testvalue"))
	if err != nil {
		t.Fatal(err)
	}
	exists = storage.Exists(ctx, key)
	if !exists {
		t.Fatalf("key should exist")
	}
}

func TestStoreUpdatesModified(t *testing.T) {
	var err error

	ctx := context.Background()
	storage := setup(ctx, t)

	key := "testkey"
	defer storage.Delete(ctx, key)

	err = storage.Store(ctx, key, []byte("0"))
	if err != nil {
		t.Fatal(err)
	}
	infoBefore, err := storage.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	err = storage.Store(ctx, key, []byte("00"))
	if err != nil {
		t.Fatal(err)
	}
	infoAfter, err := storage.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !infoBefore.Modified.Before(infoAfter.Modified) {
		t.Fatalf("modified not updated")
	}
	if int64(2) != infoAfter.Size {
		t.Fatalf("size not updated")
	}
}

func TestStoreExistsLoadDelete(t *testing.T) {
	var err error

	ctx := context.Background()
	storage := setup(ctx, t)

	key := "testkey"
	defer storage.Delete(ctx, key)

	val := []byte("testval")

	if storage.Exists(ctx, key) {
		t.Fatalf("key should not exist")
	}

	err = storage.Store(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	if !storage.Exists(ctx, key) {
		t.Fatalf("key should exist")
	}

	load, err := storage.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, load) {
		t.Fatalf("got: %s", load)
	}

	err = storage.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	load, err = storage.Load(ctx, key)
	if load != nil {
		t.Fatalf("load should be nil")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err not certmagic.ErrNotExist")
	}
}

func TestStat(t *testing.T) {
	var err error

	ctx := context.Background()
	storage := setup(ctx, t)

	key := "testkey"
	defer storage.Delete(ctx, key)

	val := []byte("testval")
	if err = storage.Store(ctx, key, val); err != nil {
		t.Fatal(err)
	}
	stat, err := storage.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Modified.IsZero() {
		t.Fatalf("modified should not be zero")
	}
	if !reflect.DeepEqual(stat, certmagic.KeyInfo{
		Key:        key,
		Modified:   stat.Modified,
		Size:       int64(len(val)),
		IsTerminal: true,
	}) {
		t.Fatalf("got: %v", stat)
	}
}

func TestList(t *testing.T) {
	var err error

	ctx := context.Background()
	storage := setup(ctx, t)
	keys := []string{
		"testnohit1",
		"testnohit2",
		"testhit1",
		"testhit2",
		"testhit3",
	}
	for _, key := range keys {
		if err = storage.Store(ctx, key, []byte("hit")); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, key := range keys {
			if err = storage.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}
		}
	}()
	list, err := storage.List(ctx, "testhit", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("got: %d", len(list))
	}
	sort.Strings(list)
	if !reflect.DeepEqual([]string{"testhit1", "testhit2", "testhit3"}, list) {
		t.Fatalf("got: %v", list)
	}
}

func TestLockLocks(t *testing.T) {
	ctx := context.Background()
	storage := setup(ctx, t)

	key := "testkey"
	defer storage.Unlock(ctx, key)

	if err := storage.Lock(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := storage.isLocked(ctx, storage.db, key); err == nil {
		t.Fatalf("key should be locked")
	}
	if err := storage.Unlock(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := storage.isLocked(ctx, storage.db, key); err != nil {
		t.Fatal(err)
	}
}

func TestLockExpires(t *testing.T) {
	ctx := context.Background()
	storage := setupWithOptions(ctx, t, Options{LockTimeout: 100 * time.Millisecond})

	key := "testkey"
	defer storage.Unlock(ctx, key)

	if err := storage.Lock(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := storage.isLocked(ctx, storage.db, key); err == nil {
		t.Fatalf("key should be locked")
	}
	time.Sleep(200 * time.Millisecond)
	if err := storage.isLocked(ctx, storage.db, key); err != nil {
		t.Fatal(err)
	}
}
