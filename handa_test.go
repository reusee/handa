package handa

import (
  "testing"
)

func getDb() *Handa {
  db := New("localhost", "3306", "root", "ffffff", "test", "45678")
  return db
}

func TestNew(t *testing.T) {
  getDb()
}

func TestSet(t *testing.T) {
  db := getDb()
  err := db.Set("thread", "tid", 15, "collect", true)
  if err != nil { t.Fail() }
  err = db.Set("thread", "tid", 16, "collect", true)
  if err != nil { t.Fail() }
  err = db.Set("thread", "tid", 17, "collect", true)
  if err != nil { t.Fail() }
  err = db.Set("thread", "tid", 18, "collect", true)
  if err != nil { t.Fail() }

  err = db.Set("thread", "tid", 16, "ccc", 18)
  if err != nil { t.Fail() }
  err = db.Set("thread", "tid", 18, "float", 5.5)
  if err != nil { t.Fail() }
  err = db.Set("thread", "tid", 19, "subject", "哈哈哈")
  if err != nil { t.Fail() }
}
