package main

import (
	"CSEQUERYGEN/services"
	"CSEQUERYGEN/utils"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"strings"
	"time"
)

func main() {
	initLoggerAndViper()
	singleInTasks := ""
	inputFilePath := ""
	println(utils.AsciiLogo)

	for strings.ToUpper(singleInTasks) != "Y" && strings.ToUpper(singleInTasks) != "N" {
		println("Use single input file for all tasks? (Y/N)")
		_, err := fmt.Scanln(&singleInTasks)
		if err != nil {
			log.Err(err).Msg("")
			os.Exit(1)
		}
		log.Info().Msgf("got task type from user %s", singleInTasks)
	}

	if strings.ToUpper(singleInTasks) == "Y" {
		getInputFilePath(&inputFilePath)
	}

	queriesCount := viper.GetInt("number_of_query")
	queryTasks := services.GetQueryTasks(queriesCount)
	for _, task := range queryTasks {

		if strings.ToUpper(singleInTasks) == "N" {
			getInputFilePath(&inputFilePath)
		}
		services.DoQueryTasks(task, inputFilePath)
	}
}

func getInputFilePath(inputFilePath *string) {
	println("Enter complete path to the input CSV file:")
	_, err := fmt.Scanln(inputFilePath)
	if err != nil {
		log.Err(err).Msg("")
		os.Exit(1)
	}
	log.Info().Msgf("got input file path: %s", *inputFilePath)
}

func initLoggerAndViper() {

	//log config
	logFile := &lumberjack.Logger{
		Filename:   "logs/query.log",
		MaxSize:    50, // MB
		MaxBackups: 3,  // Number of backups to keep
		//MaxAge:     1,  // Days
		Compress: true,
	}
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	multi := zerolog.MultiLevelWriter(os.Stdout, logFile)
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()

	//viper config to read config files
	viper.SetConfigName("querygen")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
