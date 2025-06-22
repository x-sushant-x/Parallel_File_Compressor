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
	semaphore   chan struct{}
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
				buf := make([]byte, config.ChunkSize)
				return &buf
			},
		},
		semaphore: make(chan struct{}, config.MaxMemoryUsage/config.ChunkSize),
	}
}

func (pc *ParallelCompressor) Start() {
	for i := 0; i < pc.config.NumWorkers; i++ {
		pc.workerWG.Add(1)
		go pc.worker()
	}

	pc.resultWG.Add(1)
	go pc.processResult()
}

func (pc *ParallelCompressor) worker() {
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
	result := &CompressionResult{Job: job}

	startTime := time.Now()
	defer func() { result.Duration = time.Since(startTime) }()

	inputFile, err := os.Open(job.InputPath)
	if err != nil {
		return setError(result, err)
	}
	defer inputFile.Close()

	result.OriginalSize = getFileSize(inputFile)

	outputFile, err := os.Create(job.OutputPath)
	if err != nil {
		return setError(result, err)
	}
	defer outputFile.Close()

	if err := pc.compress(inputFile, outputFile); err != nil {
		return setError(result, err)
	}

	result.CompressedSize = getFileSize(outputFile)
	return result
}

func (pc *ParallelCompressor) compress(inputFile io.Reader, outputFile io.Writer) error {
	select {
	case pc.semaphore <- struct{}{}:
	case <-pc.ctx.Done():
		return fmt.Errorf("context cancelled while waiting for memory slot")
	}

	defer func() {
		<-pc.semaphore
	}()

	gzipWriter, err := gzip.NewWriterLevel(outputFile, pc.config.CompressionLevel)
	if err != nil {
		return err
	}
	defer gzipWriter.Close()

	bufferPtr := pc.bufferPool.Get().(*[]byte)
	buffer := *bufferPtr
	defer pc.bufferPool.Put(bufferPtr)

	for {
		n, err := inputFile.Read(buffer)
		if n > 0 {
			if _, writeErr := gzipWriter.Write(buffer[:n]); writeErr != nil {
				return writeErr
			}
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}
	}

	return err
}

func setError(r *CompressionResult, err error) *CompressionResult {
	r.Error = err
	return r
}

func getFileSize(file *os.File) int64 {
	stat, err := file.Stat()
	if err != nil {
		return 0
	}
	return stat.Size()
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
			}

		case <-pc.ctx.Done():
			return
		}
	}
}

func main() {
	printMemUsage("Before")

	config := &CompressionConfig{
		NumWorkers:       8,
		MaxMemoryUsage:   32 * 1024 * 1024,
		CompressionLevel: 9,
		ChunkSize:        2 * 1024 * 1024,
	}

	compressor := NewParallelCompressor(config)

	compressor.Start()

	files := []string{}

	for i := 1; i <= 13; i++ {
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

	fmt.Printf("Time Taken: %d milliseconds\n\n", t.Milliseconds())

	printMemUsage("After")

}

func printMemUsage(label string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("\n--- %s ---\n", label)
	fmt.Printf("Alloc = %v MiB", m.Alloc/1024/1024)
	fmt.Printf("\tTotalAlloc = %v MiB", m.TotalAlloc/1024/1024)
	fmt.Printf("\tSys = %v MiB", m.Sys/1024/1024)

	fmt.Println()
	fmt.Println()
}
