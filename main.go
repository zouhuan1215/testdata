package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	addr       = flag.String("addr", "127.0.0.1:4000", "tidb-server addr, default: :4000")
	dbName     = flag.String("db", "tpch", "db name, default: tpch")
	user       = flag.String("u", "root", "user, default: root")
	pwd        = flag.String("p", "", "password, default: empty")
	sqlFile    = flag.String("query", "/tmp/queries/", "SQL data file for bench, default: /tmp/queries")
	idxFile    = flag.String("idx", "/tmp/indexadvisor/1_RESULT", "recommend index file, default: /tmp/indexadvisor/RIDX")
	outputPath = flag.String("res", "/tmp/indexadvisor", "file path to write execution time of qury, default: /tmp/indexadvisor")
	topn       = flag.Int("cnt", 3, "the number of desired indexes added to original table, default: 3")
)

var (
	db *sql.DB
)

const (
	queryChanSize    int = 10000
	writeResChanSize int = 10000
)

func init() {
	flag.Parse()
	var err error
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", *user, *pwd, *addr, *dbName))
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	fmt.Printf("****************[Start]**********************\n")
	execOrigin := path.Join(*outputPath, "EXEC_ORIGIN")
	ExecQuery(*sqlFile, execOrigin)
	fmt.Printf("Exec original query done!\n")

	//	idxOut := path.Join(*outputPath, "EXEC_BUILDINDEX")
	//	AddVirutalIndex(*idxFile, idxOut, *topn)
	//	fmt.Printf("Build virtual index done!\n")

	execWithIdx := path.Join(*outputPath, "EXEC_ADDIDX")
	ExecQuery(*sqlFile, execWithIdx)
	fmt.Printf("After building index, exec query done!\n")
}

func ExecQuery(infile, outfile string) {
	queryChan := make(chan string, queryChanSize)
	writeResChan := make(chan string, writeResChanSize)
	wg := sync.WaitGroup{}
	wgRes := sync.WaitGroup{}
	wg.Add(1)
	go worker(queryChan, writeResChan, &wg)

	wgRes.Add(1)
	go writeResWorker(writeResChan, outfile, &wgRes)

	go readQuery(queryChan, infile)

	wg.Wait()

	close(writeResChan)
	wgRes.Wait()

	return
}

func AddVirutalIndex(infile, outfile string, cnt int) {
	cmdChan := make(chan string, queryChanSize)
	writeResChan := make(chan string, writeResChanSize)
	wg := sync.WaitGroup{}
	wgRes := sync.WaitGroup{}

	wg.Add(1)
	go worker(cmdChan, writeResChan, &wg)

	wgRes.Add(1)
	go writeResWorker(writeResChan, outfile, &wgRes)

	go buildDDLStmt(cmdChan, infile, cnt)
	wg.Wait()

	close(writeResChan)
	wgRes.Wait()

	return
}

func buildDDLStmt(cmdChan chan string, infile string, n int) {
	defer close(cmdChan)

	lines, err := readFile(infile)
	if err != nil {
		panic(err)
	}

	if len(lines) < n {
		n = len(lines)
	}

	for i := 0; i < n; i++ {
		res := strings.Fields(lines[i])
		if len(res) != 3 {
			panic("split error")
		}
		tbl := res[0][:len(res[0])-1]
		idx := res[1]
		cmdChan <- fmt.Sprintf("create index virtual_index_%d on %s%s;", i+1, tbl, idx)
	}
}

func readFile(filename string) (lines []string, err error) {
	fp, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	r := bufio.NewReader(fp)
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
		} else {
			lines = append(lines, string(line))
		}
	}
	return
}

func writeResWorker(writeResChan chan string, outfile string, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		content, ok := <-writeResChan
		if !ok {
			return
		}
		fd, err := os.OpenFile(outfile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		defer fd.Close()

		fd.WriteString(content)
	}
}

func readQuery(queryChan chan string, readPath string) {
	defer close(queryChan)

	files, err := ioutil.ReadDir(readPath)
	if err != nil {
		panic(err)
	}

	n := len(files)

	for i := 1; i <= n; i++ {
		sqlfile := readPath + strconv.Itoa(i) + ".sql"

		if exists(sqlfile) {
			contents, err := ioutil.ReadFile(sqlfile)
			if err != nil {
				panic(err)
			}
			queryChan <- string(contents)
		}
	}
}

func worker(queryChan, writeResChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	var cnt uint64 = 0
	for {
		query, ok := <-queryChan
		if !ok {
			// No more query
			return
		}
		cnt++
		fmt.Printf("============================[%v]========================\n", cnt)
		fmt.Printf("%v\n", query)
		exec(query, cnt, writeResChan)
	}
}

func exec(sqlStmt string, cnt uint64, writeResChan chan string) error {
	sql := strings.ToLower(sqlStmt)
	isQuery := strings.HasPrefix(sql, "select")

	// Get time
	startTs := time.Now()
	err := runQuery(sqlStmt, isQuery)
	if err != nil {
		panic(err)
	}
	spend := time.Now().Sub(startTs).Seconds()
	res := fmt.Sprintf("%-10d%f\n", cnt, spend)
	writeResChan <- res
	return nil
}

func runQuery(sqlStmt string, isQuery bool) error {
	if isQuery {
		_, err := db.Query(sqlStmt)
		if err != nil {
			return err
		}
		return nil
	}

	_, err := db.Exec(sqlStmt)
	return err
}

func exists(file string) bool {
	_, err := os.Stat(file)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}
