package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type Chunk struct {
	start int64
	len   int64
}

type Summary struct {
	min   float32
	max   float32
	count int64
	sum   float32
}

func NewSummary(temp float32) Summary {
	return Summary{
		min:   temp,
		max:   temp,
		count: 1,
		sum:   temp,
	}
}

func (s *Summary) Add(temp float32) {
	if temp < s.min {
		s.min = temp
	}

	if temp > s.max {
		s.max = temp
	}

	s.count++
	s.sum += temp
}

func (s *Summary) Merge(s2 *Summary) {
	if s2.min < s.min {
		s.min = s2.min
	}

	if s2.max > s.max {
		s.max = s2.max
	}

	s.count += s2.count
	s.sum += s2.sum
}

type ProcessingInfo struct {
	chunkNum int64
	data     map[string]Summary
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	filePath := "./measurements.txt"

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open a file %v", filePath)
	}

	defer file.Close()

	chunks := createChunks(file)

	doneCh := make(chan ProcessingInfo)

	for i := 0; i < len(chunks); i++ {
		go reading(int64(i), file, chunks[i], doneCh)
	}

	res := make(map[string]Summary, 10000)

	for i := 0; i < len(chunks); i++ {
		data := <-doneCh
		fmt.Printf("\ndone processing chunk: %v", data)

		for k, v := range data.data {
			val, ok := res[k]

			if ok {
				val.Merge(&v)
			} else {
				res[k] = v
			}
		}
	}

	fmt.Printf("\n\n\n{")
	for k, v := range res {
		fmt.Printf("\"%v\":\"%v/%v/%v\",", k, v.min, v.max, (v.sum / float32(v.count)))
		// fmt.Printf("%v=%v/%v/%v", k, v.min, v.max, (v.sum / float32(v.count)))
	}
	fmt.Printf("}")
}

func createChunks(file *os.File) []Chunk {
	info, err := file.Stat()

	if err != nil {
		log.Fatalf("failed to read file info %v", err)
	}

	fmt.Printf("\nfile size: %v", info.Size())

	fileSize := info.Size()
	var chunksCount int64 = 40
	chunkSize := info.Size() / chunksCount

	chunks := make([]Chunk, chunksCount)

	var currPos int64 = 0

	i := 0
	for ; i < int(chunksCount); i++ {
		if currPos+chunkSize >= fileSize {

			chunks[i] = Chunk{
				start: currPos,
				len:   fileSize - currPos,
			}
			i++
			break
		}

		// fmt.Printf("\ncurr: %v, %v", currPos, currPos+chunkSize)

		file.Seek(0, 0)
		seekReader := io.NewSectionReader(file, currPos+chunkSize, fileSize-(currPos+chunkSize))
		reader := bufio.NewReader(seekReader)

		counter := 0
		for {
			c, err := reader.ReadByte()
			if err != nil && err != io.EOF {
				log.Fatalf("unable to read a byte. %v, %v, %v", err, err == io.EOF, c)
			}

			// fmt.Printf("\n%c", c)

			counter++
			if c == byte('\n') || err == io.EOF {
				break
			}
		}

		chunks[i] = Chunk{
			start: currPos,
			len:   (currPos + chunkSize + int64(counter)) - currPos,
		}

		fmt.Printf("\nudpated chunk : %v, %v, %v", chunks[i].start, chunks[i].len, chunks[i].start+chunks[i].len)

		currPos = currPos + chunkSize + int64(counter)
	}

	chunks = chunks[:i]

	prev := chunks[0]
	for i := 1; i < len(chunks); i++ {
		curr := chunks[i]

		if prev.start+prev.len != curr.start {
			log.Panicf("\nBad chunks")
		}

		if i == len(chunks)-1 && curr.start+curr.len != fileSize {
			log.Panicf("\nBad last chunk")
		}
		prev = curr
	}

	fmt.Printf("\nchunks: %v", chunks)

	return chunks
}

func reading(i int64, file *os.File, chunk Chunk, done chan ProcessingInfo) {
	seekReader := io.NewSectionReader(file, chunk.start, chunk.len)
	reader := bufio.NewReader(seekReader)

	res := make(map[string]Summary)

	for {
		line, readLineErr := reader.ReadString('\n')

		if readLineErr != nil && readLineErr != io.EOF {
			log.Fatalf("\nerror reading line from chunk %v, err: %v", i, readLineErr)
		}

		if readLineErr == io.EOF && len(line) == 0 {

			break
		}

		parts := strings.Split(line, ";")
		name := strings.TrimSpace(parts[0])
		tempStr := strings.TrimSpace(parts[1])
		temp, err := strconv.ParseFloat(tempStr, 32)

		if err != nil {
			log.Fatalf("\nunable to parse temp: %v = %v", name, tempStr)
		}

		// fmt.Printf("\n %v > %v = %v", i, name, temp)

		val, ok := res[name]

		if ok {
			val.Add(float32(temp))
		} else {
			res[name] = NewSummary(float32(temp))
		}

		if readLineErr == io.EOF {
			break
		}
	}

	done <- ProcessingInfo{
		chunkNum: i,
		data:     res,
	}
}

// func main() {
// 	filePath := "../../github/1brc/measurements.txt"

// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		log.Fatalf("failed to open a file %v", filePath)
// 	}
// 	defer file.Close()

// 	chunks := createChunks(file)

// 	var wg sync.WaitGroup
// 	doneCh := make(chan ProcessingInfo, len(chunks))

// 	for i, chunk := range chunks {
// 		wg.Add(1)
// 		go reading(int64(i), file, chunk, doneCh, &wg)
// 	}

// 	go func() {
// 		wg.Wait()
// 		close(doneCh)
// 	}()

// 	res := make(map[string]Summary, 10000)

// 	for data := range doneCh {
// 		fmt.Printf("\ndone processing chunk: %v", data)

// 		for k, v := range data.data {
// 			if existing, ok := res[k]; ok {
// 				existing.Merge(&v)
// 			} else {
// 				res[k] = v
// 			}
// 		}
// 	}

// 	fmt.Printf("\n\n\n{")
// 	for k, v := range res {
// 		fmt.Printf("\"%v\":\"%v/%v/%v\",", k, v.min, v.max, (v.sum / float32(v.count)))
// 	}
// 	fmt.Printf("}")
// }

// func createChunks(file *os.File) []Chunk {
// 	info, err := file.Stat()
// 	if err != nil {
// 		log.Fatalf("failed to read file info %v", err)
// 	}

// 	fmt.Printf("\nfile size: %v", info.Size())

// 	fileSize := info.Size()
// 	var chunksCount int64 = 20
// 	chunkSize := info.Size() / chunksCount

// 	chunks := make([]Chunk, chunksCount)
// 	currPos := int64(0)

// 	for i := range chunks {
// 		start := currPos
// 		end := start + chunkSize

// 		// Adjust end for the last chunk
// 		if i == len(chunks)-1 {
// 			end = fileSize
// 		}

// 		chunks[i] = Chunk{
// 			start: start,
// 			len:   end - start,
// 		}

// 		currPos = end
// 	}

// 	fmt.Printf("\nchunks: %v", chunks)
// 	return chunks
// }

// func reading(i int64, file *os.File, chunk Chunk, done chan ProcessingInfo, wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	seekReader := io.NewSectionReader(file, chunk.start, chunk.len)
// 	reader := bufio.NewReader(seekReader)

// 	res := make(map[string]Summary)

// 	for {
// 		line, readLineErr := reader.ReadString('\n')
// 		// ... (rest of your reading logic)

// 		if readLineErr == io.EOF && len(line) == 0 {
// 			break
// 		}

// 		if readLineErr == io.EOF {
// 			break
// 		}
// 	}

// 	done <- ProcessingInfo{
// 		chunkNum: i,
// 		data:     res,
// 	}
// }
