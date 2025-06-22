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
		jobQueue:    make(chan *Job, 100),
		resultQueue: make(chan *CompressionResult, 100),
		ctx:         ctx,
		cancel:      cancel,
		bufferPool: sync.Pool{
			New: func() any {
				return make([]byte, config.ChunkSize*1024*1024)
			},
		},
	}
}

func (pc *ParallelCompressor) Start() {
	for i := range pc.config.NumWorkers {
		pc.workerWG.Add(1)
		go pc.worker(i)
	}
}

func (pc *ParallelCompressor) worker(workerID int) {
	defer pc.workerWG.Done()

	for {
		select {
		case job := <-pc.jobQueue:
			if job == nil {
				return
			}

			result := pc.processJob(job)

			select {
			case pc.resultQueue <- result:
			case <-pc.ctx.Done():
				return
			}

		case <-pc.ctx.Done():
			return
		}
	}
}

func (pc *ParallelCompressor) processJob(job *Job) *CompressionResult {
	result := &CompressionResult{
		Job: job,
	}

	startTime := time.Now()
	defer func() {
		result.Duration = time.Since(startTime)
	}()

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

	outputFileStat, _ := outputFile.Stat()

	result.CompressedSize = outputFileStat.Size()
	return result
}

func main() {}
