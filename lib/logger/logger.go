package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// Settings stores config for logger
type Settings struct {
	Path string `yaml:"path"`
	Name string `yaml:"name"`
	Ext string `yaml:"ext"`
	TimeFormat string `yaml:"time-format"`
}

var (
	logFile *os.File
	defaultPrefix = ""
	defaultCallerDepth = 2
	logger *log.Logger
	mu sync.Mutex
	logPrefix = ""
	levelFlags = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
)

type logLevel int

// log levels
const (
	DEBUG logLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

const flags = log.LstdFlags

func init() {
	logger = log.New(os.Stdout, defaultPrefix, flags)
}

// Setup initializes logger
func Setup(settings *Settings) {
	var err error
	dir := settings.Path
	fileName := fmt.Sprintf("%s-%s.%s",
		settings.Name,
		time.Now().Format(settings.TimeFormat),
		settings.Ext)

	logFile, err := mustOpen(fileName, dir)
	if err != nil {
		log.Fatalf("logging.Setup err: %s", err)
	}

	mw := io.MultiWriter(os.Stdout, logFile)
	logger = log.New(mw, defaultPrefix, flags)
}