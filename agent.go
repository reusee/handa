package handa

func (self *Handa) Update(table string, index string, key interface{}, fieldList string, values ...interface{}) (count int, change int, err error) {
  return self.NewCursor(false).Update(table, index, key, fieldList, values...)
}

func (self *Handa) Insert(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  return self.NewCursor(false).Insert(table, index, key, fieldList, values...)
}

func (self *Handa) InsertUpdate(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  return self.NewCursor(false).InsertUpdate(table, index, key, fieldList, values...)
}

func (self *Handa) UpdateInsert(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  return self.NewCursor(false).UpdateInsert(table, index, key, fieldList, values...)
}

func (self *Handa) GetCol(table string, index string) ([]string, error) {
  return self.NewCursor(false).GetCol(table, index)
}

func (self *Handa) GetMap(table string, index string, field string) (map[string]string, error) {
  return self.NewCursor(false).GetMap(table, index, field)
}
