/*
	Author:
		Nicholas Siow | n.siow@wustl.edu

	Description:
		Super-fast gzip reader that uses goroutine pools to
		eliminate bottlenecks in filereading
*/

package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

//--------------------------------------------------------------------------------
//	program setup
//--------------------------------------------------------------------------------

// which command to use for reading gzip files
var Unzipper string = "gzcat"

// semaphore throttling constants
var ParserPool int = 6
var ReducerPool int = 2

// logging objects
var (
	Debugging_on bool
	Debug        *log.Logger
	Warning      *log.Logger
	Error        *log.Logger
)

//--------------------------------------------------------------------------------
//	program utility and helper functions
//--------------------------------------------------------------------------------

/*
	function to initialize the logging objects
*/
func LogInit() {
	if Debugging_on {
		Debug = log.New(os.Stdout, "[DEBUG] ", 0)
		Warning = log.New(os.Stdout, "[WARNING] ", 0)
		Error = log.New(os.Stdout, "[ERROR] ", 0)
	} else {
		Debug = log.New(ioutil.Discard, "[DEBUG] ", 0)
		Warning = log.New(ioutil.Discard, "[WARNING] ", 0)
		Error = log.New(ioutil.Discard, "[ERROR] ", 0)
	}
}

//--------------------------------------------------------------------------------
//	Reader class which handles reading the binary data from disk and
//	decompressing it
//--------------------------------------------------------------------------------

type Reader struct {
	filename string
	bsize    int
	outq     chan []byte
}

func (self Reader) GetReader() io.Reader {
	if strings.HasSuffix(self.filename, ".gz") {
		c := exec.Command(Unzipper, "-c", self.filename)
		pipe, err := c.StdoutPipe()
		if err != nil {
			panic(err)
		}
		c.Start()
		return pipe
	} else {
		// open the file in read-only mode
		file, err := os.Open(self.filename)
		if err != nil {
			Error.Fatalln(err)
		}

		// create and return reader object
		return io.Reader(file)
	}
}

func (self Reader) Start() {
	reader := self.GetReader()

	// wrap that with a bufio reader
	bsize := self.bsize
	var leftovers []byte
	for {
		// read in the next chunk
		buffer := make([]byte, bsize)
		length, err := reader.Read(buffer)
		if err != nil && err != io.EOF {
			Error.Fatalln(err)
		}

		// break if reading is done
		if length == 0 {
			break
		}

		// truncate newlines at end
		end_it := length - 1
		for {
			if end_it == 0 {
				leftovers = append(leftovers, buffer...)
				break
			}
			if buffer[end_it] == '\n' {
				leftovers = append(leftovers, buffer[:end_it]...)
				self.outq <- leftovers
				leftovers = buffer[end_it+1:]
				break
			} else {
				end_it -= 1
			}
		}
	}

	// close channel to let next worker know that you're done
	close(self.outq)
}

//--------------------------------------------------------------------------------
//	Parser class which handles splitting data at newlines and separating
//	into key->value map
//--------------------------------------------------------------------------------

type conn struct {
	orig  string
	resp  string
	bytes int
}

type Parser struct {
	limiter chan int
	inq     chan []byte
	outq    chan []conn
}

func (self Parser) Parse(fileslice []byte) {
	lines := strings.Split(string(fileslice), "\n")

	data_slice := make([]conn, len(lines))
	for _, line := range lines {
		if line[0] == '#' {
			continue
		}
		data := strings.Split(line, "\t")
		orig := data[2]
		resp := data[4]
		bytes1, _ := strconv.Atoi(data[16])
		bytes2, _ := strconv.Atoi(data[18])
		data_slice = append(data_slice, conn{orig, resp, bytes1 + bytes2})
	}

	self.outq <- data_slice
	<-self.limiter
}

func (self Parser) Start() {
	for fileslice := range self.inq {
		self.limiter <- 1
		go self.Parse(fileslice)
	}

	for {
		if len(self.limiter) == 0 {
			close(self.outq)
			break
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

//--------------------------------------------------------------------------------
//	Reducer class which performs some data reduction over the data
//--------------------------------------------------------------------------------

type Reducer struct {
	limiter chan int
	inq     chan []conn
	outq    chan map[string]int64
}

func (self Reducer) Reduce(data_slice []conn) {
	tt := make(map[string]int64)

	for _, c := range data_slice {
		orig := c.orig
		resp := c.resp
		b := c.bytes

		if strings.HasPrefix(orig, "128.252.") {
			tt[orig] += int64(b)
		}

		if strings.HasPrefix(resp, "128.252.") {
			tt[resp] += int64(b)
		}
	}

	self.outq <- tt
	<-self.limiter
}

func (self Reducer) Start() {
	for data_slice := range self.inq {
		self.limiter <- 1
		go self.Reduce(data_slice)
	}

	for {
		if len(self.limiter) == 0 {
			close(self.outq)
			break
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

//--------------------------------------------------------------------------------
//	Combiner class that sums the partiai results created by the reducer
//--------------------------------------------------------------------------------

type Combiner struct {
	inq  chan map[string]int64
	outq chan int
}

func (self Combiner) Start() {
	final := make(map[string]int64)

	for subresult := range self.inq {
		for ip, bytecount := range subresult {
			final[ip] += bytecount
		}

		self.outq <- len(subresult)
	}

	self.Report(final)
	close(self.outq)
}

// define custom interface for sorting int64
type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

func (self Combiner) Report(tt map[string]int64) {
	keys := make([]int64, len(tt))
	swapped := make(map[int64][]string)

	var tbytes int64
	for k, v := range tt {
		list, ok := swapped[v]
		if ok {
			list = append(list, k)
		} else {
			swapped[v] = []string{k}
		}

		keys = append(keys, v)
		tbytes += int64(v)
	}

	// sort the keys in descending order
	var sortable_keys int64arr
	sortable_keys = keys
	sort.Sort(sort.Reverse(sortable_keys))

	num_printed := 0
	for _, k := range keys {
		ips := swapped[k]
		keepgoing := true
		for _, ip := range ips {
			if num_printed < 10 {
				fmt.Printf("%15v %.4f%%\n", ip, float64(k)/float64(tbytes)*100)
				num_printed += 1
			} else {
				keepgoing = false
				break
			}
		}

		if !keepgoing {
			break
		}
	}
}

//--------------------------------------------------------------------------------
//	main program body
//--------------------------------------------------------------------------------

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// parse cmd-line flags
	var filename = flag.String("f", "", "the gzip file to be parsed")
	var bsize = flag.Int("b", -1, "specify the blocksize to be used in filereading")
	var debugging = flag.Bool("d", false, "turn on program debugging")
	flag.Parse()

	// use options to initalize loggers
	Debugging_on = *debugging
	LogInit()

	// make sure given options are valid
	if *filename == "" {
		Error.Fatalln("Please specify a file to process with the <-f> flag.")
	}

	if *bsize <= 0 {
		Error.Fatalf("Invalid blocksize given: %d", *bsize)
	}

	// print some debugging information
	Debug.Printf("Received cmdline arguments:")
	Debug.Printf("\tfilename: %v", *filename)
	Debug.Printf("\tbsize: %v", *bsize)
	Debug.Printf("\tdebugging: %v", *debugging)

	// create the necessary channels
	chansize := 10000
	chan1 := make(chan []byte, chansize)
	chan2 := make(chan []conn, chansize)
	chan3 := make(chan map[string]int64, chansize)
	chan4 := make(chan int, chansize)

	// create buffered controller channels that can act as semaphores
	// to limit overall throughput
	limiter1 := make(chan int, ParserPool)
	limiter2 := make(chan int, ReducerPool)

	// intialize the various worker objects
	r := Reader{*filename, *bsize, chan1}
	p := Parser{limiter1, chan1, chan2}
	rd := Reducer{limiter2, chan2, chan3}
	c := Combiner{chan3, chan4}

	// start each of the worker functions on its own goroutine
	go r.Start()
	go p.Start()
	go rd.Start()
	go c.Start()

	// loop and monitor status of workers
	fmtstring := "\rReader -> (%d) -> Parser -> (%d) -> Reducer -> (%d) -> Combiner -> (%d done)"
	fmtstring = fmt.Sprintf("%70s", fmtstring)
	sampler := 0
	total_done := 0
	for my_done := range chan4 {
		total_done += my_done
		if sampler%10000 == 0 {
			fmt.Printf(fmtstring, len(chan1), len(chan2), len(chan3), total_done)
		}
		sampler += 1
	}
}
