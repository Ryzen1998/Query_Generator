package services

import (
	"CSEQUERYGEN/models"
	"encoding/csv"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"os"
	"strconv"
	"strings"
	"time"
)

func GetQueryTasks(queriesCount int) []models.CSEQueryModel {
	var queryTasks []models.CSEQueryModel

	for i := 0; i < queriesCount; i++ {
		csvFieldsString := strings.Split(viper.GetString(fmt.Sprintf("query%d.csv_fields", i+1)), ",")

		csvFieldsIntSlice := func() []int {
			var fields []int
			for _, str := range csvFieldsString {
				i, err := strconv.Atoi(str)
				if err != nil {
					log.Err(err).Msg("")
					continue
				}
				fields = append(fields, i)
			}
			return fields
		}()

		resp, err := models.NewQueryModel(
			viper.GetString(fmt.Sprintf("query%d.table_name", i+1)),
			viper.GetString(fmt.Sprintf("query%d.query", i+1)),
			csvFieldsIntSlice,
		)
		if err != nil {
			log.Err(err).Msg("")
			continue
		}

		queryTasks = append(queryTasks, *resp)
	}

	return queryTasks
}

func DoQueryTasks(task models.CSEQueryModel, inputFile string) {

	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		fmt.Printf("error reading file: %s", err)
		panic(err)
	}

	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	outputFile, err := os.Create(fmt.Sprintf("%s/%s%s.sql", viper.GetString("output_sql_path"),
		task.TableName, time.Now().Format("200601021504")))
	if err != nil {
		log.Err(err).Msg("error creating file")
		return
	}
	defer outputFile.Close()

	for idx, record := range records {
		fields, err := task.GetCSVFields(record)
		if err != nil {
			log.Warn().Msgf("skipped %d due to invalid fields", idx)
			continue
		}
		_, err = fmt.Fprintf(outputFile, task.SqlQuery, fields...)
		if err != nil {
			log.Err(err).Msg("error writing to file")
		}
	}
}
