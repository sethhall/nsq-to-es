// This is an NSQ client that reads the specified topic/channel
// and performs HTTP requests (GET/POST) to the specified endpoints

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	//"math/rand"
	//"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
)

const (
	ModeAll = iota
	ModeRoundRobin
	ModeHostPool
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic       = flag.String("topic", "bro_logs", "nsq topic")
	channel     = flag.String("channel", "nsq_to_elasticsearch", "nsq channel")
	maxInFlight = flag.Int("max-in-flight", 10, "max number of messages to allow in flight")

	numPublishers = flag.Int("n", 1, "number of concurrent publishers")
	mode          = flag.String("mode", "round-robin", "the upstream request mode options: round-robin or hostpool")
	httpTimeout   = flag.Duration("http-timeout", 20*time.Second, "timeout for HTTP connect/read/write (each)")
	statusEvery   = flag.Int("status-every", 250, "the # of requests between logging status (per handler), 0 disables")
	contentType   = flag.String("content-type", "text/json", "the Content-Type used for POST requests")

	readerOpts       = util.StringArray{}
	postAddrs        = util.StringArray{}
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}

	maxBackoffDuration = flag.Duration("max-backoff-duration", 120*time.Second, "(deprecated) use --reader-opt=max_backoff_duration=X, the maximum backoff duration")
	bulkSize           = flag.Int("bulk-size", 1000, "the # of messages to forward to elasticsearch in each bulk transfer")
	maxBulkTime        = flag.Duration("max-bulk-time", 60*time.Second, "the maximum number of seconds between bulk inserts")

	queuedItems    []string
	lastBulkTime       = time.Now()
	queuedItemsNum int = 0
)

func init() {
	flag.Var(&readerOpts, "reader-opt", "option to passthrough to nsq.Reader (may be given multiple times)")
	flag.Var(&postAddrs, "post", "URL to make a POST request to.  data will be in the body (may be given multiple times)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")

	queuedItems = make([]string, *bulkSize)
}

type Durations []time.Duration

func (s Durations) Len() int {
	return len(s)
}

func (s Durations) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Durations) Less(i, j int) bool {
	return s[i] < s[j]
}

type Publisher interface {
	Publish(string, []byte) error
}

type PublishHandler struct {
	Publisher
	addresses util.StringArray
	counter   uint64
	mode      int
	hostPool  hostpool.HostPool
	reqs      Durations
	id        int
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message) error {
	var startTime time.Time

	if *statusEvery > 0 {
		startTime = time.Now()
	}

	endOfCommand := strings.Index(string(m.Body), "}}") + 2
	CommandString := m.Body[0:endOfCommand]
	LogString := m.Body[endOfCommand:]

	//log.Printf("stuff: %s", m.Body)
	//log.Printf("stuff: %s", CommandString)
	//log.Printf("stuff: %s", LogString)
	//tmp := fmt.Sprintf("%s\n%s\n", CommandString, )
	queuedItems = append(queuedItems, string(CommandString))
	queuedItems = append(queuedItems, string(LogString))
	queuedItemsNum = queuedItemsNum + 1

	//log.Printf("seconds? %d", lastBulkTime.Unix())
	sinceLastBulk := time.Now().Sub(lastBulkTime)

	//fmt.Printf("sincelastbulk: %s   maxbulksecs: %s\n", sinceLastBulk, *maxBulkTime)
	//fmt.Printf("bulksize: %d\n", *bulkSize)
	fmt.Printf("Length of queued items: %d\n", queuedItemsNum)
	if queuedItemsNum >= *bulkSize || (queuedItemsNum > 0 && sinceLastBulk > *maxBulkTime) {
		log.Printf("last batch time: %s number of items %d\n", sinceLastBulk, queuedItemsNum)
		lastBulkTime = time.Now()

		log.Printf("begin preparing string\n")

		//out := string("")
		out := strings.Join(queuedItems[2:queuedItemsNum*2], "\n") + "\n"
		//for e := queuedItems.Front(); e != nil; e = e.Next() {
		//	out = fmt.Sprintf("%s%s\n", out, e.Value)
		//}

		// If the data sent, flush the queuedItems
		queuedItemsNum = 0
		queuedItems = make([]string, *bulkSize)

		log.Printf("done preparing string\n")

		switch ph.mode {
		case ModeAll:
			for _, addr := range ph.addresses {
				err := ph.Publish(addr, []byte(out))
				if err != nil {
					return err
				}
			}
		case ModeRoundRobin:
			idx := ph.counter % uint64(len(ph.addresses))
			err := ph.Publish(ph.addresses[idx], []byte(out))
			if err != nil {
				return err
			}
			ph.counter++
		case ModeHostPool:
			hostPoolResponse := ph.hostPool.Get()
			err := ph.Publish(hostPoolResponse.Host(), []byte(out))
			hostPoolResponse.Mark(err)
			if err != nil {
				return err
			}
		}

		log.Printf("finished pushing logs\n")

		if *statusEvery > 0 {
			duration := time.Now().Sub(startTime)
			ph.reqs = append(ph.reqs, duration)
		}

		if *statusEvery > 0 && len(ph.reqs) >= *statusEvery {
			var total time.Duration
			for _, v := range ph.reqs {
				total += v
			}
			avgMs := (total.Seconds() * 1000) / float64(len(ph.reqs))

			sort.Sort(ph.reqs)
			p95Ms := percentile(95.0, ph.reqs, len(ph.reqs)).Seconds() * 1000
			p99Ms := percentile(99.0, ph.reqs, len(ph.reqs)).Seconds() * 1000

			log.Printf("handler(%d): finished %d requests - 99th: %.02fms - 95th: %.02fms - avg: %.02fms",
				ph.id, *statusEvery, p99Ms, p95Ms, avgMs)

			ph.reqs = ph.reqs[:0]
		}
	}

	return nil
}

func percentile(perc float64, arr []time.Duration, length int) time.Duration {
	indexOfPerc := int(math.Ceil(((perc / 100.0) * float64(length)) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}

type PostPublisher struct{}

func (p *PostPublisher) Publish(addr string, msg []byte) error {
	buf := bytes.NewBuffer(msg)
	resp, err := HttpPost(addr, buf)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New(fmt.Sprintf("got status code %d", resp.StatusCode))
	}
	return nil
}

func hasArg(s string) bool {
	for _, arg := range os.Args {
		if strings.Contains(arg, s) {
			return true
		}
	}
	return false
}

func main() {
	var publisher Publisher
	var addresses util.StringArray
	var selectedMode int

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_elasticsearch v%s\n", util.BINARY_VERSION)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if *contentType != flag.Lookup("content-type").DefValue {
		if len(postAddrs) == 0 {
			log.Fatalf("--content-type only used with --post")
		}
		if len(*contentType) == 0 {
			log.Fatalf("--content-type requires a value when used")
		}
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	switch *mode {
	case "round-robin":
		selectedMode = ModeRoundRobin
	case "hostpool":
		selectedMode = ModeHostPool
	}

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	if len(postAddrs) > 0 {
		publisher = &PostPublisher{}
		addresses = postAddrs
	}

	r, err := nsq.NewReader(*topic, *channel)
	if err != nil {
		log.Fatalf(err.Error())
	}
	err = util.ParseReaderOpts(r, readerOpts)
	if err != nil {
		log.Fatalf(err.Error())
	}
	r.SetMaxInFlight(*maxInFlight)

	for i := 0; i < *numPublishers; i++ {
		handler := &PublishHandler{
			Publisher: publisher,
			addresses: addresses,
			mode:      selectedMode,
			reqs:      make(Durations, 0, *statusEvery),
			id:        i,
			hostPool:  hostpool.New(addresses),
		}
		r.AddHandler(handler)
	}

	for _, addrString := range nsqdTCPAddrs {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdHTTPAddrs {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for {
		select {
		case <-r.ExitChan:
			return
		case <-termChan:
			r.Stop()
		}
	}
}
