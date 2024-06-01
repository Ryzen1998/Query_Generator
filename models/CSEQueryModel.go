package models

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"strings"
)

type CSEQueryModel struct {
	TableName string
	SqlQuery  string
	CSVFields []int
}

func NewQueryModel(tableName string, sqlQuery string, csvFields []int) (*CSEQueryModel, error) {
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}
	if sqlQuery == "" {
		return nil, errors.New("sql query cannot be empty")
	}
	stringHCount := strings.Count(sqlQuery, "%s")
	numberHCount := strings.Count(sqlQuery, "%d")

	if stringHCount+numberHCount != len(csvFields) {
		return nil, errors.New("the place holders count in the query does not match with csv_fields count")
	}
	return &CSEQueryModel{tableName, sqlQuery, csvFields}, nil
}

func (c *CSEQueryModel) GetCSVFields(record []string) ([]any, error) {
	var fields []any
	for _, index := range c.CSVFields {
		if index >= 0 && index < len(record) {
			fields = append(fields, record[index])
		} else {
			log.Error().Msgf("index %d out of range for record with length %d", index, len(record))
			return nil, errors.New("index out of range")
		}
	}
	return fields, nil
}
