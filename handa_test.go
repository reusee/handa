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
  err := db.Insert("thread", "tid", n, "subject", "OK")
  if err != nil {
    t.Fatal("insert error: %s", err)
  }
  err = db.InsertUpdate("thread", "tid", n, "subject", "YES")
  if err != nil {
    t.Fatal("InsertUpdate error: %s", err)
  }
  nStr := fmt.Sprintf("%d", n)
  m, err := db.GetFilteredMap("thread", "tid", "subject", "tid=" + nStr)
  if err != nil {
    t.Fatal("get filtered map error: %s", err)
  }
  if m[nStr] != "YES" {
    t.Fatal("update fail")
  }
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

func TestTextIndexInsert(t *testing.T) {
  tablename := fmt.Sprintf("test_%d", rand.Int63())
  err := db.Insert(tablename, "textcol", "TEXT", "")
  if err != nil {
    t.Fatal("insert error")
  }
  db.Insert(tablename, "textcol", "TEXT", "")
  rows, _, _ := db.mysqlQuery("SELECT COUNT(*) FROM %s", tablename)
  num := rows[0].Int(0)
  if num != 1 {
    t.Fail()
  }

  n := 10
  for i := 0; i < n; i++ {
    db.Insert(tablename, "textcol", strconv.Itoa(rand.Intn(2000000)), "")
  }
  rows, _, _ = db.mysqlQuery("SELECT COUNT(*) FROM %s", tablename)
  num = rows[0].Int(0)
  if num != n + 1 {
    t.Fail()
  }
}

func TestTextIndexUpdate(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  err := db.Insert(table, "key", "KEY", "")
  if err != nil {
    t.Fatal("insert fail")
  }
  count, change, err := db.Update(table, "key", "KEY", "foo", "FOO")
  if err != nil {
    t.Fatal("update operation error", table, err)
  }
  if count != 1 || change != 1 {
    t.Fatal("update fail", table)
  }
}

func TestHashColumnUpdate(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  key2Value := "hello"
  err := db.Insert(table, "key1", "value1", "key2", key2Value)
  if err != nil {
    t.Fatal("insert error")
  }
  c, _, err := db.Update(table, "key2", key2Value, "key1", "newValue1")
  if err != nil {
    t.Fatal("update error")
  }
  if c != 1 {
    t.Fatal("update fail", table)
  }
  err = db.Insert(table, "key1", "newValue1", "") // will fail
  if err == nil {
    t.Fatal("consistency error")
  }
  m, err := db.GetFilteredMap(table, "key2", "key1", "key2=" + key2Value)
  if err != nil {
    t.Fatal("get map error: %s", err)
  }
  if m[key2Value] != "newValue1" {
    t.Fatal("not update")
  }
}

func TestMultiColumnIndex(t *testing.T) {
  err := db.InsertUpdate("price", "itemid, time", []interface{}{1, 20121009}, "price", 5.5)
  if err != nil {
    t.Fatal(err)
  }
  err = db.InsertUpdate("price", "itemid, time", []interface{}{1, 20121008}, "price", 6.5)
  if err != nil {
    t.Fatal(err)
  }
  cols, err := db.GetFilteredCol("price", "price", "itemid=1", "time=20121009")
  if len(cols) != 1 || cols[0] != "5.5" {
    t.Fail()
  }
  cols, err = db.GetFilteredCol("price", "price", "itemid=1", "time=20121008")
  if len(cols) != 1 || cols[0] != "6.5" {
    t.Fail()
  }
}

func TestGetRangedCol(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  for i := 0; i < 10; i++ {
    db.Insert(table, "n", i, "")
  }
  res, err := db.GetRangedCol(table, "n", 0, 5)
  if err != nil {
    t.Fail()
  }
  if len(res) != 5 {
    t.Fail()
  }
  for i := 0; i < len(res); i++ {
    resN, _ := strconv.Atoi(res[i])
    if resN != i {
      t.Fail()
    }
  }
}

func TestTextFieldFilter(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  db.Insert(table, "c1", 5, "c2,c3", "foo", "bar")
  db.Insert(table, "c1", 6, "c2,c3", "foo", "bar")
  db.Insert(table, "c1", 7, "c2,c3", "foo", "bar")
  db.Insert(table, "c1", 8, "c2,c3", "foo", "bar")
  rows, err := db.GetFilteredCol(table, "c1", "c2=foo")
  if err != nil {
    t.Fatal("GetFilteredCol fail", err)
  }
  if len(rows) != 4 {
    t.Fail()
  }
}
