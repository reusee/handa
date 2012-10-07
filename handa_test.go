package handa

import (
  "testing"
  "time"
  "math/rand"
  "fmt"
  tdh "github.com/reusee/go-tdhsocket"
)

func getDb() *Handa {
  db := New("localhost", "3306", "test", "ffffff", "test", "45678")
  fmt.Printf("")
  rand.Seed(time.Now().UnixNano())
  return db
}

func TestNew(t *testing.T) {
  getDb()
}

func TestUpdate(t *testing.T) {
  db := getDb()
  count, change, err := db.Update("thread", "tid", 432142314, "collect", true)
  if err != nil {
    t.Fail()
  }
  if count != 0 || change != 0 {
    t.Fail()
  }
}

func TestInsert(t *testing.T) {
  db := getDb()
  for i := 0; i < 10; i++ {
    n := rand.Int63()
    err := db.Insert("thread", "tid", n, "collect", true)
    if err != nil {
      t.Fail()
    }
    err = db.Insert("thread", "tid", n, "collect", true)
    if err == nil {
      t.Fail()
    }
    if err.(*tdh.Error).ClientStatus != 502 || err.(*tdh.Error).ErrorCode != 121 {
      t.Fail()
    }
  }
}

func TestUpdateInsert(t *testing.T) {
  db := getDb()
  err := db.UpdateInsert("thread", "tid", 15, "collect", true)
  if err != nil { t.Fail() }
  err = db.UpdateInsert("thread", "tid", 16, "collect", true)
  if err != nil { t.Fail() }
  err = db.UpdateInsert("thread", "tid", 17, "collect", true)
  if err != nil { t.Fail() }
  err = db.UpdateInsert("thread", "tid", 18, "collect", true)
  if err != nil { t.Fail() }

  err = db.UpdateInsert("thread", "tid", 16, "ccc", 18)
  if err != nil { t.Fail() }
  err = db.UpdateInsert("thread", "tid", 18, "float", 5.5)
  if err != nil { t.Fail() }
  err = db.UpdateInsert("thread", "tid", 19, "subject", "哈哈哈")
  if err != nil { t.Fail() }
}

func TestInsertUpdate(t *testing.T) {
  db := getDb()
  n := rand.Int63()
  db.Insert("thread", "tid", n, "subject", "OK")
  db.InsertUpdate("thread", "tid", n, "subject", "YES")
  //TODO fetch and check
}

func BenchmarkUpdateInsert(b *testing.B) {
  b.StopTimer()
  db := getDb()
  b.StartTimer()
  for i := 0; i < b.N; i++ {
    db.UpdateInsert("thread", "tid", time.Now().UnixNano(), "subject,ccc,float,collect", "好！", rand.Int31(), rand.Float64(), true)
  }
}

func BenchmarkInsertUpdate(b *testing.B) {
  b.StopTimer()
  db := getDb()
  b.StartTimer()
  for i := 0; i < b.N; i++ {
    db.InsertUpdate("thread", "tid", time.Now().UnixNano(), "subject,ccc,float,collect", "好！", rand.Int31(), rand.Float64(), true)
  }
}
