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
  db.Set("thread", "tid", 15, "collect", true)
  db.Set("thread", "tid", 16, "collect", true)
  db.Set("thread", "tid", 17, "collect", true)
  db.Set("thread", "tid", 18, "collect", true)

  db.Set("thread", "tid", 16, "ccc", 18)
  db.Set("thread", "tid", 18, "float", 5.5)
  db.Set("thread", "tid", 19, "subject", "哈哈哈")
}
