package handa

import (
  "log"
  "fmt"
  "strings"

  tdh "github.com/reusee/go-tdhsocket"
  "github.com/reusee/mmh3"

  "github.com/ziutek/mymysql/autorc"
  "github.com/ziutek/mymysql/mysql"
  _ "github.com/ziutek/mymysql/native" 
)

var (
  MysqlConnPoolSize = 16
  SocketConnPoolSize = 64
)

type Handa struct {
  mysqlConnPool chan *autorc.Conn
  socketConnPool chan *tdh.Conn

  dbname string
  schema map[string]*TableInfo
}

func New(host string, port string, user string, password string, database string, tdhPort string) *Handa {
  self := new(Handa)

  // init database connection pool
  self.dbname = database
  self.mysqlConnPool = make(chan *autorc.Conn, MysqlConnPoolSize)
  for i := 0; i < MysqlConnPoolSize; i++ {
    conn := autorc.New("tcp", "", host + ":" + port, user, password, database)
    conn.Register("set names utf8")
    self.mysqlConnPool <- conn
  }
  self.socketConnPool = make(chan *tdh.Conn, SocketConnPoolSize)
  for i := 0; i < SocketConnPoolSize; i++ {
    socket, err := tdh.New(host + ":" + tdhPort, "", "")
    if err != nil {
      fatal("Tdhsocket connect error")
    }
    self.socketConnPool <- socket
  }

  // load table schemas
  schema := make(map[string]*TableInfo)
  row, _, _ := self.mysqlQuery("SHOW TABLES")
  for _, row := range row {
    tableName := row.Str(0)
    schema[tableName] = self.loadTableInfo(tableName)
  }
  self.schema = schema

  return self
}

func (self *Handa) loadTableInfo(tableName string) *TableInfo {
  //TODO load multiple column unique keys
  tableInfo := &TableInfo{
    name: tableName,
    columnType: make(map[string]int),
    index: make(map[string]bool),
  }
  r, _, _ := self.mysqlQuery("DESCRIBE %s", tableName)
  for _, c := range r {
    columnName := c.Str(0)
    columnType := c.Str(1)
    columnKeyType := c.Str(3)
    switch columnType {
    case "tinyint(1)":
      tableInfo.columnType[columnName] = ColTypeBool
    case "bigint(255)":
      tableInfo.columnType[columnName] = ColTypeInt
    case "double":
      tableInfo.columnType[columnName] = ColTypeFloat
    case "longtext", "longblob":
      tableInfo.columnType[columnName] = ColTypeString
    case "char(32)":
      tableInfo.columnType[columnName] = ColTypeHash
    }
    if columnKeyType == "UNI" {
      tableInfo.index[columnName] = true
    }
  }
  return tableInfo
}

const (
  ColTypeBool = iota
  ColTypeInt
  ColTypeFloat
  ColTypeString
  ColTypeHash
)

type TableInfo struct {
  name string
  columnType map[string]int
  index map[string]bool
}

func (self *Handa) mysqlQuery(sql string, args ...interface{}) ([]mysql.Row, mysql.Result, error) {
  conn := <-self.mysqlConnPool
  defer func() {
    self.mysqlConnPool <- conn
  }()
  return conn.Query(sql, args...)
}

func (self *Handa) checkSchemaAndConvertData(table string, indexesStr string, keys interface{},
  fieldList string, values ...interface{}) (dbIndex string, 
  dbIndexStrs []string, dbKeys []string,
  indexStrs []string, keyStrs []string,
  dbFields []string, dbValues []string) {

  // table
  self.ensureTableExists(table)

  // index and key
  // split indexes
  // convert keys to string
  // ensure key columns exists
  // ensure index exists

  indexStrs = strings.Split(indexesStr, ",")
  for i, indexStr := range indexStrs {
    indexStrs[i] = strings.TrimSpace(indexStr)
  }
  dbKeys = make([]string, 0)
  keyStrs = make([]string, 0)
  dbIndexStrs = make([]string, 0)
  if len(indexStrs) == 1 {
    keyStr, t := convertToString(keys)
    dbKey := keyStr
    index := indexStrs[0]
    self.ensureColumnExists(table, index, t)
    var isString []bool
    dbIndex, isString = self.ensureIndexExists(table, index)
    if isString[0] {
      dbKey = mmh3Hex(dbKey)
    }
    dbKeys = append(dbKeys, dbKey)
    keyStrs = append(keyStrs, keyStr)
    dbIndexStrs = append(dbIndexStrs, dbIndex)
  } else { // multiple column index
  }

  // fields and values
  fields := strings.Split(fieldList, ",")
  if len(fields) == 1 && fields[0] == "" {
    fields = nil
  }
  dbFields = make([]string, 0)
  dbValues = make([]string, 0)
  for i, field := range fields {
    dbField := strings.TrimSpace(field)
    dbFields = append(dbFields, dbField)
    dbValue, t := convertToString(values[i])
    dbValues = append(dbValues, dbValue)
    self.ensureColumnExists(table, dbField, t)
    if t == ColTypeString {
      fieldHashField := "hash_" + field
      if _, hasHashColumn := self.schema[table].columnType[fieldHashField]; hasHashColumn {
        dbFields = append(dbFields, fieldHashField)
        dbValues = append(dbValues, mmh3Hex(dbValue))
      }
    }
  }
  return
}

func (self *Handa) withTableCacheOff(fun func()) {
  _, _, err := self.mysqlQuery("SET GLOBAL tdh_socket_cache_table_on=0")
  defer self.mysqlQuery("SET GLOBAL tdh_socket_cache_table_on=1")
  if err != nil {
    panic("need SUPER privileges to set global variable")
  }
  fun()
}

func (self *Handa) ensureTableExists(table string) {
  _, exists := self.schema[table]
  if !exists {
    self.withTableCacheOff(func() {
      self.mysqlQuery(`CREATE TABLE IF NOT EXISTS %s (
        serial SERIAL
      ) engine=InnoDB`, table)
    })
    self.schema[table] = self.loadTableInfo(table)
  }
}

func (self *Handa) ensureColumnExists(table string, column string, t int) (created bool) {
  if column == "serial" {
    return
  }
  _, exists := self.schema[table].columnType[column]
  if !exists {
    var columnType string
    switch t {
    case ColTypeBool:
      columnType = "BOOLEAN"
    case ColTypeInt:
      columnType = "BIGINT(255)"
    case ColTypeFloat:
      columnType = "DOUBLE"
    case ColTypeString:
      columnType = "LONGTEXT"
    case ColTypeHash:
      columnType = "CHAR(32)"
    }
    self.withTableCacheOff(func() {
      self.mysqlQuery("ALTER TABLE `%s` ADD (`%s` %s NULL DEFAULT NULL)", table, column, columnType)
    })
    created = true
    self.schema[table] = self.loadTableInfo(table)
  }
  return
}

func (self *Handa) ensureIndexExists(table string, columns ...string) (indexName string, isString []bool) {
  if len(columns) == 1 && columns[0] == "serial" {
    indexName = "serial"
    isString = append(isString, false)
    return
  }
  indexSubnames := make([]string, len(columns))
  isString = make([]bool, len(columns))
  for i, column := range columns {
    indexSubname := column
    if self.schema[table].columnType[column] == ColTypeString {
      indexSubname = "hash_" + column
      isString[i] = true
    }
    indexSubnames[i] = indexSubname
  }
  indexName = strings.Join(indexSubnames, "$")
  if !self.schema[table].index[indexName] { // create index
    quotedColumns := make([]string, len(indexSubnames))
    for i, t := range isString {
      if t { // ensure hash column exists
        created := self.ensureColumnExists(table, indexSubnames[i], ColTypeHash)
        if created { // update hashes
          data, err := self.GetMap(table, "serial", columns[i])
          if err != nil {
            log.Fatal("get data error ", err)
          }
          batch := self.Batch()
          for k, v := range data {
            batch.Update(table, "serial", k, indexSubnames[i], mmh3Hex(v))
          }
          batch.Commit()
        }
      }
      quotedColumns[i] = "`" + indexSubnames[i] + "`"
    }
    self.withTableCacheOff(func() {
      _, _, err := self.mysqlQuery("CREATE UNIQUE INDEX `%s` ON `%s` (%s)",
        indexName, table, strings.Join(quotedColumns, ","))
      if err != nil {
        log.Fatal("index creation error: %s", err)
      }
    })
    self.schema[table] = self.loadTableInfo(table)
  }
  return
}

func (self *Handa) NewCursor(isBatch bool) *Cursor {
  cursor := &Cursor{
    isValid: true,
    handa: self,
    isBatch: isBatch,
    end: make(chan bool, 1),
  }
  init := make(chan bool, 1)
  go func() {
    conn := <-self.socketConnPool
    defer func() {
      self.socketConnPool <- conn
    }()
    cursor.conn = conn
    if cursor.isBatch {
      cursor.conn.Batch()
    }
    init <- true
    <-cursor.end
    cursor.isValid = false
  }()
  <-init
  return cursor
}

func (self *Handa) Batch() *Cursor {
  return self.NewCursor(true)
}

func fatal(format string, args ...interface{}) {
  log.Fatal(fmt.Sprintf(format, args...))
}

func mmh3Hex(s string) string {
  return fmt.Sprintf("%x", string(mmh3.Hash128([]byte(s))))
}
