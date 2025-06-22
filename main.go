package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

type CompressionConfig struct {
	NumWorkers       int
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

	pc.resultWG.Add(1)
	go pc.processResult()
}

func (pc *ParallelCompressor) worker(workerID int) {
	defer pc.workerWG.Done()

	for {
		select {
		case job := <-pc.jobQueue:
			if job == nil {
				return
			}

			fmt.Printf("Job Picked By Worker: %d\n", workerID)
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

func (pc *ParallelCompressor) processResult() {
	defer pc.resultWG.Done()

	for {
		select {
		case result := <-pc.resultQueue:
			if result == nil {
				return
			}

			if result.Error != nil {
				fmt.Printf("Result Error: %s\n", result.Error.Error())
			} else {
				fmt.Printf("Compression Successfull: %s\n", result.Job.OutputPath)
			}

		case <-pc.ctx.Done():
			return
		}
	}
}

func main() {
	config := &CompressionConfig{
		NumWorkers:       0,
		MaxMemoryUsage:   4 * 1024 * 1024,
		CompressionLevel: 10,
		ChunkSize:        4,
	}

	compressor := NewParallelCompressor(config)

	compressor.Start()

	files := []string{}

	for i := 1; i <= 11; i++ {
		files = append(files, fmt.Sprintf("testdata/book%d.pdf", i))
	}

	now := time.Now()

	for _, inputPath := range files {
		outputPath := inputPath + "_compressed"

		job := &Job{
			InputPath:  inputPath,
			OutputPath: outputPath,
			Ctx:        context.Background(),
		}

		compressor.jobQueue <- job
	}

	close(compressor.jobQueue)
	compressor.workerWG.Wait()

	close(compressor.resultQueue)
	compressor.resultWG.Wait()

	t := time.Since(now)

	fmt.Printf("Time Taken: %d\n", t.Milliseconds())

	fmt.Println("All compression jobs completed.")
}
