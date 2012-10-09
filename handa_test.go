package handa

import (
  "testing"
  "time"
  "math/rand"
  "fmt"
  tdh "github.com/reusee/go-tdhsocket"
  "sync"
  "strconv"
)

var db *Handa

func TestInit(t *testing.T) {
  db = New("localhost", "3306", "test", "ffffff", "test", "45678")
  fmt.Printf("")
  rand.Seed(time.Now().UnixNano())
}

func TestUpdate(t *testing.T) {
  count, change, err := db.Update("thread", "tid", 432142314, "collect", true)
  if err != nil {
    t.Fail()
  }
  if count != 0 || change != 0 {
    t.Fail()
  }
}

func TestInsert(t *testing.T) {
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
  n := rand.Int63()
  db.Insert("thread", "tid", n, "subject", "OK")
  db.InsertUpdate("thread", "tid", n, "subject", "YES")
  //TODO fetch and check
}

func BenchmarkUpdateInsert(b *testing.B) {
  for i := 0; i < b.N; i++ {
    db.UpdateInsert("thread", "tid", time.Now().UnixNano(), "subject,ccc,float,collect", "好！", rand.Int31(), rand.Float64(), true)
  }
}

func BenchmarkInsertUpdate(b *testing.B) {
  for i := 0; i < b.N; i++ {
    db.InsertUpdate("thread", "tid", time.Now().UnixNano(), "subject,ccc,float,collect", "好！", rand.Int31(), rand.Float64(), true)
  }
}

func BenchmarkBatchInsertUpdate(b *testing.B) {
  b.StopTimer()
  c := db.Batch()
  b.StartTimer()
  for i := 0; i < b.N; i++ {
    c.InsertUpdate("thread", "tid", time.Now().UnixNano(), "subject,ccc,float,collect", "batch好！", rand.Int31(), rand.Float64(), true)
  }
  c.Commit()
}

func TestBatchCursor(t *testing.T) {
  c := db.Batch()
  c.Insert("thread", "tid", rand.Int63(), "subject", "insert in batch")
  c.Commit()
}

func TestBatchComparison(t *testing.T) {
  n := 20

  startTime := time.Now()
  for i := 0; i < n; i++ {
    err := db.Insert("thread", "tid", rand.Int63(), "subject,ccc,float,collect", "comp no batch", rand.Int31(), rand.Float64(), true)
    if err != nil {
      t.Fail()
    }
  }
  fmt.Printf("Not batch %v\n", time.Now().Sub(startTime))

  c := db.Batch()
  startTime = time.Now()
  for i := 0; i < n; i++ {
    c.Insert("thread", "tid", rand.Int63(), "subject,ccc,float,collect", "comp batch", rand.Int31(), rand.Float64(), true)
  }
  res, err := c.Commit()
  if err != nil {
    t.Fail()
  }
  for _, r := range res {
    if r.Err != nil {
      t.Fail()
    }
  }
  fmt.Printf("Batch %v\n", time.Now().Sub(startTime))

  startTime = time.Now()
  wg := new(sync.WaitGroup)
  for i := 0; i < n; i++ {
    wg.Add(1)
    go func() {
      defer wg.Done()
      err := db.Insert("thread", "tid", rand.Int63(), "subject,ccc,float,collect", "comp conn", rand.Int31(), rand.Float64(), true)
      if err != nil {
        t.Fail()
      }
    }()
  }
  wg.Wait()
  fmt.Printf("Use goroutine %v\n", time.Now().Sub(startTime))
}

func TestGetCol(t *testing.T) {
  res, err := db.GetCol("thread", "tid")
  if err != nil {
    t.Fail()
  }
  if !(len(res) > 0) {
    t.Fail()
  }
}

func TestGetMap(t *testing.T) {
  res, err := db.GetMap("thread", "tid", "collect")
  if err != nil {
    t.Fail()
  }
  if !(len(res) > 0) {
    t.Fail()
  }
}

func TestGetFilteredCol(t *testing.T) {
  res, err := db.GetFilteredCol("thread", "tid", "tid>50", "collect=0")
  if err != nil {
    fmt.Printf("%s\n", err)
    t.Fail()
  }
  if !(len(res) > 0) {
    t.Fail()
  }
  for _, r := range res {
    tid, _ := strconv.Atoi(r)
    if tid <= 50 {
      t.Fail()
    }
  }
}

func TestGetFilteredMap(t *testing.T) {
  res, err := db.GetFilteredMap("thread", "tid", "collect", "collect=0")
  if err != nil {
    t.Fail()
  }
  if !(len(res) > 0) {
    t.Fail()
  }
  for _, v := range res {
    if v != "0" {
      t.Fail()
    }
  }
}

func TestDDLWhenBatch(t *testing.T) {
  n := rand.Int31()
  b := db.Batch()
  for i := 0; i < 10; i++ {
    b.Insert("thread", "tid", rand.Int63(), "foo_" + strconv.Itoa(int(n)), "OK")
  }
  res, err := b.Commit()
  if err != nil {
    t.Fail()
  }
  for _, r := range res {
    if r.Err != nil {
      t.Fail()
    }
  }
}

func TestEmptyFieldList(t *testing.T) {
  db.Insert("thread", "tid", rand.Int63(), "")
}

func TestTextTypeIndex(t *testing.T) {
  tablename := fmt.Sprintf("test_%d", rand.Int31())
  db.Insert(tablename, "textcol", "TEXT", "")
}
