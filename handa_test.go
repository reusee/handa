package handa

import (
  "testing"
  "time"
  "math/rand"
  "fmt"
  tdh "github.com/reusee/go-tdhsocket"
  "sync"
  "strconv"
  "strings"
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

  for i := 0; i < 10; i++ {
    db.Insert(table, "n", i + 20, "foo", "bar")
  }
  res, err = db.GetRangedFilteredCol(table, "n", 1, 5, "foo=bar")
  if err != nil {
    t.Fail()
  }
  if len(res) != 5 {
    t.Fail()
  }
}

func TestGetRangedMap(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  for i := 0; i < 10; i++ {
    db.Insert(table, "i", i, "s", strings.Repeat("OK", i))
  }
  res, err := db.GetRangedMap(table, "i", "s", 1, 7)
  if err != nil {
    t.Fail()
  }
  for i := 1; i <= 7; i++ {
    if res[strconv.Itoa(i)] != strings.Repeat("OK", i) {
      t.Fail()
    }
  }

  for i := 30; i < 40; i++ {
    db.Insert(table, "i", i, "s,p", strings.Repeat("FOO", i), "FOO")
  }
  res, err = db.GetRangedFilteredMap(table, "i", "s", 3, 5, "p=FOO")
  if err != nil {
    t.Fail()
  }
  for i := 33; i < 38; i++ {
    if res[strconv.Itoa(i)] != strings.Repeat("FOO", i) {
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

func TestGetMultiCol(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  db.Insert(table, "c1", 1, "c2,c3", "FOO", "BAR")
  res, err := db.GetMultiCol(table, "c1, c2, c3")
  if err != nil {
    t.Fail()
  }
  if len(res) != 1 {
    t.Fail()
  }
  if res[0][0] != "1" || res[0][1] != "FOO" || res[0][2] != "BAR" {
    t.Fail()
  }
}

func TestGetMultiFilteredCol(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  db.Insert(table, "c1", 1, "c2,c3", "FOO", "BAR")
  db.Insert(table, "c1", 2, "c2,c3", "Foo", "Bar")
  res, err := db.GetMultiFilteredCol(table, "c1, c2, c3", "c1=2")
  if err != nil {
    t.Fail()
  }
  if len(res) != 1 {
    t.Fail()
  }
  if res[0][0] != "2" || res[0][1] != "Foo" || res[0][2] != "Bar" {
    t.Fail()
  }
}

func TestGetMultiRangedCol(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  for i := 0; i < 10; i++ {
    db.Insert(table, "i", i, "foo,bar", "FOO", "BAR")
  }
  res, err := db.GetMultiRangedCol(table, "i,foo,bar", 3, 5)
  if err != nil {
    t.Fail()
  }
  if len(res) != 5 {
    t.Fail()
  }
  for i := 0; i < 5; i++ {
    row := res[i]
    if row[0] != strconv.Itoa(i + 3) {
      t.Fail()
    }
    if row[1] != "FOO" || row[2] != "BAR" {
      t.Fail()
    }
  }
}

func TestGetMultiRangedFilteredCol(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  for i := 0; i < 10; i++ {
    db.Insert(table, "i", i, "c2,c3,p", "FOO", "BAR", "P1")
    db.Insert(table, "i", i + 20, "c2,c3,p", "Foo", "Bar", "P2")
  }
  res, err := db.GetMultiRangedFilteredCol(table, "i,c2,c3", 4, 5, "p=P2")
  if err != nil {
    t.Fatal("get error")
  }
  if len(res) != 5 {
    t.Fatal("result number not match")
  }
  for i := 0; i < 5; i++ {
    row := res[i]
    if row[0] != strconv.Itoa(i + 20 + 4) {
      t.Fatal("i not match")
    }
    if row[1] != "Foo" || row[2] != "Bar" {
      t.Fatal("column not match")
    }
  }
}

func TestGetMultiMap(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  for i := 0; i < 5; i++ {
    db.Insert(table, "i", i, "a,b", strings.Repeat("A", i), strings.Repeat("B", i))
  }
  m, err := db.GetMultiMap(table, "i", "a,b")
  if err != nil {
    t.Fatal("GetMultiMap fail")
  }
  if len(m) != 5 {
    t.Fatal("len error")
  }
  for i := 0; i < 5; i++ {
    key := strconv.Itoa(i)
    if m[key][0] != strings.Repeat("A", i) || m[key][1] != strings.Repeat("B", i) {
      t.Fatal("value error")
    }
  }
}

func TestGetMultiFilteredMap(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  for i := 0; i < 5; i++ {
    db.Insert(table, "i", i, "a,b", strings.Repeat("A", i), strings.Repeat("B", i))
  }
  m, err := db.GetMultiFilteredMap(table, "i", "a,b", "a=AAA")
  if err != nil {
    t.Fatal("GetMultiFilteredMap error")
  }
  if m["3"] == nil {
    t.Fatal("get fail")
  }
  if m["3"][1] != "BBB" {
    t.Fatal("field value error")
  }
}

func TestGetMultiRangedMap(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  for i := 0; i < 5; i++ {
    db.Insert(table, "i", i, "a,b", strings.Repeat("A", i), strings.Repeat("B", i))
  }
  m, err := db.GetMultiRangedMap(table, "i", "a,b", 2, 3)
  if err != nil {
    t.Fatal("GetMultiRangedMap error")
  }
  for i := 2; i < 5; i++ {
    key := strconv.Itoa(i)
    if m[key] == nil {
      t.Fatal("get error")
    }
    if len(m[key]) != 2 {
      t.Fatal("field number error")
    }
    if m[key][0] != strings.Repeat("A", i) || m[key][1] != strings.Repeat("B", i) {
      t.Fatal("field value error")
    }
  }
}

func TestGetMultiRangedFilteredMap(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  for i := 0; i < 5; i++ {
    db.Insert(table, "i", i, "a,b,p", strings.Repeat("a", i), strings.Repeat("b", i), "foo")
    db.Insert(table, "i", i + 30, "a,b,p", strings.Repeat("A", i), strings.Repeat("B", i), "bar")
  }
  m, err := db.GetMultiRangedFilteredMap(table, "i", "a,b", 2, 3, "p=bar")
  if err != nil {
    t.Fatal("GetMultiRangedFilteredMap error")
  }
  if len(m) != 3 {
    t.Fatal("len error")
  }
  for i := 0; i < 3; i++ {
    key := strconv.Itoa(i + 30 + 2)
    if m[key] == nil {
      t.Fatal("key error")
    }
    if len(m[key]) != 2 {
      t.Fatal("value len error")
    }
    if m[key][0] != strings.Repeat("A", i + 2) || m[key][1] != strings.Repeat("B", i + 2) {
      t.Fatal("value error")
    }
  }
}

func TestDefaultValue(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  db.Insert(table, "k", 1, "b,i,f,t,s", true, 5, 5.5, "hello", []byte("hello"))
  db.Insert(table, "k", 2, "")
  fmt.Printf("TestDefaultValue table %s\n", table)
}

func TestMulindex(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  for id := 1; id <= 10; id++ {
    db.Insert(table, "id,time", []interface{}{id, 1000}, "price", id * 2)
    db.Insert(table, "id,time", []interface{}{id, 2000}, "price", id * 3)
    db.Insert(table, "id,time", []interface{}{id, 3000}, "price", id * 4)
  }

  res, err := db.GetMultiCol(table, "id$time, id, time")
  if err != nil { t.Fatal(err) }
  i := 0
  for id := 1; id <= 10; id++ {
    for _, time := range []int{1000, 2000, 3000} {
      if res[i][0] != strconv.Itoa(id) || res[i][1] != strconv.Itoa(time) {
        t.Fatal("GetMultiCol")
      }
      i++
    }
  }

  res2, err := db.GetFilteredCol(table, "id$time, time", "id=3")
  if err != nil { t.Fatal(err) }
  for i, time := range []int{1000, 2000, 3000} {
    if res2[i] != strconv.Itoa(time) {
      t.Fatal("GetFilteredCol")
    }
  }

  res3, err := db.GetMultiFilteredCol(table, "id$time, time, price", "id=2")
  if err != nil { t.Fatal(err) }
  prices := []int{4, 6, 8}
  for i, time := range []int{1000, 2000, 3000} {
    if res3[i][0] != strconv.Itoa(time) || res3[i][1] != strconv.Itoa(prices[i]) {
      t.Fatal("GetMultiFilteredCol")
    }
  }

  m, err := db.GetFilteredMap(table, "id$time, time", "price", "id=3")
  if err != nil { t.Fatal(err) }
  times := []string{"1000", "2000", "3000"}
  prices2 := []string{"6", "9", "12"}
  for i, time := range times {
    if m[time] != prices2[i] {
      t.Fatal("GetFilteredMap")
    }
  }

  m2, err := db.GetMultiFilteredMap(table, "id$time, time", "price, time", "id=3")
  if err != nil { t.Fatal(err) }
  for i, time := range times {
    if m2[time][0] != prices2[i] || m2[time][1] != time {
      t.Fatal("GetMultiFilteredMap")
    }
  }
}

func TestConcurrentDDL(t *testing.T) {
  table := fmt.Sprintf("test_%d", rand.Int63())
  wg := new(sync.WaitGroup)
  wg.Add(10)
  for i := 0; i < 10; i++ {
    i := i
    go func() {
      db.Insert(table, "concurrent_column_creating", i, "")
      wg.Done()
    }()
  }
  wg.Wait()
  rows, err := db.GetCol(table, "concurrent_column_creating")
  if err != nil {
    t.Fatal("get error")
  }
  if len(rows) != 10 {
    t.Fatal("insert error")
  }
}
