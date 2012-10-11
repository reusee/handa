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

func (self *Handa) GetFilteredCol(table string, index string, filters ...string) ([]string, error) {
  return self.NewCursor(false).GetFilteredCol(table, index, filters...)
}

func (self *Handa) GetRangedCol(table string, index string, start int, limit int) ([]string, error) {
  return self.NewCursor(false).GetRangedCol(table, index, start, limit)
}

func (self *Handa) GetRangedFilteredCol(table string, index string, start int, limit int, filters ...string) ([]string, error) {
  return self.NewCursor(false).GetRangedFilteredCol(table, index, start, limit, filters...)
}

func (self *Handa) GetMap(table string, index string, field string) (map[string]string, error) {
  return self.NewCursor(false).GetMap(table, index, field)
}

func (self *Handa) GetFilteredMap(table string, index string, field string, filters ...string) (map[string]string, error) {
  return self.NewCursor(false).GetFilteredMap(table, index, field, filters...)
}

func (self *Handa) GetRangedMap(table string, index string, field string, start int, limit int) (map[string]string, error) {
  return self.NewCursor(false).GetRangedMap(table, index, field, start, limit)
}

func (self *Handa) GetRangedFilteredMap(table string, index string, field string, start int, limit int, filters ...string) (map[string]string, error) {
  return self.NewCursor(false).GetRangedFilteredMap(table, index, field, start, limit, filters...)
}
