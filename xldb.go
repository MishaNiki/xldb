package xldb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	// _ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/common/log"
	"github.com/tealeg/xlsx"
)

const (
	// DrPostgres - driver name for PostgreSQL
	DrPostgres = "postgres"
	// DrSQLite - driver name for SQLite
	DrSQLite = "sqlite"
	// DrOracle - driver name for Oracle
	DrOracle = "oracle"
	// DrMySQL - ...
	DrMySQL = "mysql"
)

// DBConfig ...
type DBConfig struct {
	Driver    string
	User      string
	Password  string
	Host      string
	Port      string
	DBName    string
	TableName string
}

type ColType uint8

const (
	ctString ColType = iota
	ctShortInt
	ctInt
	ctBigInt
	ctNumeric
	ctFloat
	ctBigFloat
	ctDate
	ctBool
)

type ColConfig struct {
	Name string
	Type ColType
}

// XLDB ...
type XLDB struct {
	DB   *DBConfig
	Cols map[string]*ColConfig
	Data []map[string]interface{}
}

// ParseInFile ...
func ParseInFile(path string) (xldbs []*XLDB, err error) {
	xldbs = make([]*XLDB, 0, 1)

	file, err := xlsx.OpenFile(path)
	if err != nil {
		return nil, err
	}

	log.Info(len(file.Sheets))

	for _, sheet := range file.Sheets {
		xldb := &XLDB{}

		// Parse DataBase config
		xldb.DB, err = parseDBConfig(sheet)
		if err != nil {
			return nil, err
		}
		log.Info(xldb.DB)

		// Parse cols config
		xldb.Cols, err = parseColsConfig(sheet)
		if err != nil {
			return nil, err
		}
		log.Info(xldb.Cols)

		log.Info("Парсинг данных")
		// Parse data
		xldb.Data, err = parseData(sheet, xldb.Cols)
		if err != nil {
			return nil, err
		}
		log.Info("Парсинг данных")

		xldbs = append(xldbs, xldb)
	}

	for _, v := range xldbs {
		log.Info(v.DB.Driver, v.Cols)
	}

	log.Info("Парсинг окончен")
	return xldbs, nil
}

// ParseDBConfig ...
func parseDBConfig(sheet *xlsx.Sheet) (*DBConfig, error) {

	const (
		idxCol = 1

		idxDriver = iota - 1
		idxUser
		idxPassword
		idxHost
		idxPort
		idxDB
		idxTableName
	)

	dbc := &DBConfig{
		User:      strings.TrimSpace(sheet.Cell(idxUser, idxCol).String()),
		Password:  strings.TrimSpace(sheet.Cell(idxPassword, idxCol).String()),
		Host:      strings.TrimSpace(sheet.Cell(idxHost, idxCol).String()),
		Port:      strings.TrimSpace(sheet.Cell(idxPort, idxCol).String()),
		DBName:    strings.TrimSpace(sheet.Cell(idxDB, idxCol).String()),
		TableName: strings.TrimSpace(sheet.Cell(idxTableName, idxCol).String()),
	}

	switch strings.TrimSpace(sheet.Cell(idxDriver, idxCol).String()) {
	case "PostgreSQL":
		dbc.Driver = DrPostgres
	case "SQLite":
		dbc.Driver = DrSQLite
	case "Oracle":
		dbc.Driver = DrOracle
	default:
		return nil, errors.New("not valid database name")
	}

	return dbc, nil
}

func parseColsConfig(sheet *xlsx.Sheet) (map[string]*ColConfig, error) {

	const (
		idxColName  = 0
		idxColType  = 1
		idxRowStart = 8
	)

	var mapColType = map[string]ColType{
		"text":        ctString,
		"varchar":     ctString,
		"string":      ctString,
		"integer":     ctInt,
		"int":         ctInt,
		"shortint":    ctShortInt,
		"bigint":      ctBigInt,
		"int8":        ctBigInt,
		"numeric":     ctBigFloat,
		"real":        ctFloat,
		"double":      ctBigFloat,
		"bool":        ctBool,
		"date":        ctDate,
		"timestamp":   ctDate,
		"timestamptz": ctDate,
	}

	ccd := make(map[string]*ColConfig)
	for i := 0; ; i++ {
		cc := &ColConfig{}
		cc.Name = strings.TrimSpace(sheet.Cell(idxRowStart+i, idxColName).String())

		if cc.Name == "" {
			break
		}

		t := strings.ToLower(
			strings.TrimSpace(
				sheet.Cell(idxRowStart+i, idxColType).String(),
			),
		)

		ct, ok := mapColType[t]
		if !ok {
			return nil, fmt.Errorf("")
		}
		cc.Type = ct
		ccd[cc.Name] = cc
	}

	return ccd, nil
}

func parseData(sheet *xlsx.Sheet, colsConf map[string]*ColConfig) ([]map[string]interface{}, error) {

	const (
		idxColStart = 2
		idxListRow  = 0
		idxRowStart = 1
	)

	list := []*ColConfig{}
	for i := idxColStart; i < len(sheet.Cols); i++ {
		col := strings.TrimSpace(sheet.Cell(idxListRow, i).String())
		if col == "" {
			break
		}
		cf, ok := colsConf[col]
		if !ok {
			return nil, errors.New("parseData(): col name in list data")
		}
		list = append(list, cf)
	}

	data := []map[string]interface{}{}
	for i := idxRowStart; i < sheet.MaxRow; i++ {
		row := make(map[string]interface{})
		for j, cf := range list {
			switch cf.Type {
			case ctString:
				row[cf.Name] = strings.TrimSpace(sheet.Cell(i, j+idxColStart).String())
			case ctShortInt:
				val, err := sheet.Cell(i, j+idxColStart).Int()
				if err != nil {
					return nil, fmt.Errorf("not valid value with shortInt. row = %v col = %v", i, j+idxColStart)
				}
				row[cf.Name] = int8(val)
			case ctInt:
				val, err := sheet.Cell(i, j+idxColStart).Int()
				if err != nil {
					return nil, fmt.Errorf("not valid value with integer. row = %v col = %v", i, j+idxColStart)
				}
				row[cf.Name] = val
			case ctBigInt:
				val, err := sheet.Cell(i, j+idxColStart).Int64()
				if err != nil {
					return nil, fmt.Errorf("not valid value with bigint. row = %v col = %v", i, j+idxColStart)
				}
				row[cf.Name] = val
			case ctNumeric, ctBigFloat:
				val, err := sheet.Cell(i, j+idxColStart).Float()
				if err != nil {
					return nil, fmt.Errorf("not valid value with float64. row = %v col = %v", i, j+idxColStart)
				}
				row[cf.Name] = val
			case ctFloat:
				val, err := sheet.Cell(i, j+idxColStart).Float()
				if err != nil {
					return nil, fmt.Errorf("not valid value with float. row = %v col = %v", i, j+idxColStart)
				}
				row[cf.Name] = float32(val)
			case ctBool:
				row[cf.Name] = sheet.Cell(i, j+idxColStart).Bool()
			case ctDate:
				val, err := sheet.Cell(i, j+idxColStart).GetTime(true)
				if err != nil {
					return nil, fmt.Errorf("not valid value with date. row = %v col = %v", i, j+idxColStart)
				}
				row[cf.Name] = val
			}
		}
		data = append(data, row)
	}

	return data, nil
}

// Insert ...
func (xldb *XLDB) Insert() error {
	// Connect ...
	var connConf string
	switch xldb.DB.Driver {
	case DrPostgres:
		connConf = fmt.Sprintf(
			`user=%s password=%s dbname=%s host=%s port=%s sslmode=disable`,
			xldb.DB.User,
			xldb.DB.Password,
			xldb.DB.DBName,
			xldb.DB.Host,
			xldb.DB.Port,
		)
	case DrSQLite:
		connConf = xldb.DB.DBName
	case DrOracle:
		connConf = ""
	default:
		return errors.New("not found driver name")
	}

	log.Info(xldb.DB.Driver, "  ", connConf)
	db, err := sqlx.Connect(xldb.DB.Driver, connConf)
	if err != nil {
		return err
	}

	log.Info(xldb.DB.Driver, "  ", connConf)
	// Render insert query
	cols := make([]string, 0, len(xldb.Cols))
	plhCols := make([]string, 0, len(xldb.Cols))
	for _, v := range xldb.Cols {
		cols = append(cols, v.Name)
		plhCols = append(plhCols, fmt.Sprintf(":%s", v.Name))
	}

	log.Info(xldb.DB.Driver, "  ", connConf)
	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		xldb.DB.TableName,
		strings.Join(cols, ","),
		strings.Join(plhCols, ","),
	)

	log.Info(xldb.DB.Driver, "  ", connConf)

	for _, row := range xldb.Data {
		_, err = db.NamedExec(query, row)
		if err != nil {
			return err
		}
	}

	return nil
}
