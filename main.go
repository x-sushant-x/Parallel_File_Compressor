package main

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

type CompressionConfig struct {
	NumWorkers       int
	Algorithm        string
	MaxMemoryUsage   int // MegaBytes
	CompressionLevel int
	ChunkSize        int // MegaBytes
}

// Single File Compression Task
type Job struct {
	InputPath  string
	OutputPath string
	FileSize   int64
	Ctx        context.Context
}

type CompressionResult struct {
	Job                *Job
	Error              error
	OriginalSize       int64
	CompressedSize     int64
	Duration           time.Duration
	OriginalChecksum   string
	CompressedChecksum string
}

type ParallelCompressor struct {
	config      *CompressionConfig
	jobQueue    chan *Job
	resultQueue chan *CompressionResult
	workerWG    sync.WaitGroup
	resultWG    sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	bufferPool  sync.Pool
}

func NewParallelCompressor(config *CompressionConfig) *ParallelCompressor {
	if config.NumWorkers == 0 {
		config.NumWorkers = runtime.NumCPU()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ParallelCompressor{
		config:      config,
		jobQueue:    make(chan *Job),
		resultQueue: make(chan *CompressionResult),
		ctx:         ctx,
		cancel:      cancel,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, config.ChunkSize*1024*1024)
			},
		},
	}
}

func (pc *ParallelCompressor) processJob(job *Job) *CompressionResult {
	result := &CompressionResult{
		Job: job,
	}

	inputFile, err := os.Open(job.InputPath)
	if err != nil {
		result.Error = err
		return result
	}
	defer inputFile.Close()

	stats, _ := inputFile.Stat()
	size := stats.Size()

	result.OriginalSize = size

	outputFile, err := os.Create(job.OutputPath)
	if err != nil {
		result.Error = err
		return result
	}
	defer outputFile.Close()

	gzipWriter := gzip.NewWriter(outputFile)
	defer gzipWriter.Close()

	_, err = io.Copy(gzipWriter, inputFile)
	if err != nil {
		result.Error = err
		return result
	}

	return result
}

func main() {}
