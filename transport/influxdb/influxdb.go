package influxdb

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jnovack/flag"
	"github.com/netsampler/goflow2/v2/transport"
	log "github.com/sirupsen/logrus"
)

type InfluxDbDriver struct {
	influxUrl           string
	influxToken         string
	influxOrganization  string
	influxBucket        string
	influxMeasurement   string
	influxPrecision     string
	influxGZip          bool
	influxTLSSkipVerify bool
	influxLogErrors     bool
	batchSize           int
	batchData           []map[string]interface{}
	lock                *sync.RWMutex
	q                   chan bool
	maxRetries          int
	retryDelay          time.Duration
}

func (d *InfluxDbDriver) Prepare() error {
	flag.StringVar(&d.influxUrl, "transport.influxdb.url", "http://localhost:8086", "InfluxDB URL including port")
	flag.StringVar(&d.influxToken, "transport.influxdb.token", "", "InfluxDB API token")
	flag.StringVar(&d.influxOrganization, "transport.influxdb.organization", "", "InfluxDB organization containing bucket")
	flag.StringVar(&d.influxBucket, "transport.influxdb.bucket", "", "InfluxDB bucket used for writing")
	flag.StringVar(&d.influxMeasurement, "transport.influxdb.measurement", "flowdata", "InfluxDB measurement name")
	flag.StringVar(&d.influxPrecision, "transport.influxdb.precision", "ms", "InfluxDB time precision (ns, us, ms, s)")
	flag.BoolVar(&d.influxGZip, "transport.influxdb.gzip", true, "Use GZip compression")
	flag.BoolVar(&d.influxTLSSkipVerify, "transport.influxdb.skiptlsverify", false, "Insecure TLS skip verify")
	flag.BoolVar(&d.influxLogErrors, "transport.influxdb.log.errors", true, "Log InfluxDB write errors")
	flag.IntVar(&d.batchSize, "transport.influxdb.batchSize", 1000, "Batch size for sending records")
	flag.IntVar(&d.maxRetries, "transport.influxdb.maxRetries", 3, "Maximum number of retries for failed requests")
	retryDelay := flag.Int("transport.influxdb.retryDelay", 500, "Initial retry delay in milliseconds")

	if d.batchSize <= 0 {
		d.batchSize = 1000 // default batch size
	}

	if d.maxRetries <= 0 {
		d.maxRetries = 3 // default max retries
	}

	d.retryDelay = time.Duration(*retryDelay) * time.Millisecond

	return nil
}

func (d *InfluxDbDriver) Init() error {
	d.q = make(chan bool, 1)
	d.batchData = make([]map[string]interface{}, 0, d.batchSize)
	d.lock = &sync.RWMutex{}
	return nil
}

func (d *InfluxDbDriver) Send(key, data []byte) error {
	var flowData map[string]interface{}
	err := json.Unmarshal(data, &flowData)
	if err != nil {
		return err
	}

	d.lock.Lock()
	d.batchData = append(d.batchData, flowData)
	currentBatchSize := len(d.batchData)
	d.lock.Unlock()

	if currentBatchSize >= d.batchSize {
		return d.sendBatch()
	}

	return nil
}

func (d *InfluxDbDriver) sendBatch() error {
	d.lock.Lock()
	if len(d.batchData) == 0 {
		d.lock.Unlock()
		return nil
	}

	// Create a copy of batch data to process
	batchToSend := make([]map[string]interface{}, len(d.batchData))
	copy(batchToSend, d.batchData)

	// Clear current batch data
	d.batchData = d.batchData[:0]
	d.lock.Unlock()

	// Prepare data for InfluxDB
	lines := make([]string, 0, len(batchToSend))

	for _, flow := range batchToSend {
		// Create line protocol format: <measurement>[,<tag_key>=<tag_value>...] <field_key>=<field_value>[,<field_key>=<field_value>...] [<timestamp>]

		// Define tags (metadata) and fields (measurement values)
		tags := ""
		fields := ""
		timestamp := ""

		// Add common tags if available
		for _, tagKey := range []string{"Type", "SamplerAddress", "SrcAddr", "DstAddr"} {
			if val, ok := flow[tagKey]; ok && val != nil {
				tags += fmt.Sprintf(",%s=%v", tagKey, val)
			}
		}

		// Add all remaining fields
		isFirstField := true
		for k, v := range flow {
			// Skip keys already used as tags
			if k == "Type" || k == "SamplerAddress" || k == "SrcAddr" || k == "DstAddr" {
				continue
			}

			// Format value based on type
			var formattedValue string
			switch v := v.(type) {
			case string:
				formattedValue = fmt.Sprintf("\"%s\"", v)
			case bool:
				formattedValue = fmt.Sprintf("%t", v)
			case float64:
				if k == "TimeFlowStart" || k == "TimeReceived" || k == "TimeFlowEnd" {
					// Use time field as timestamp
					timestamp = fmt.Sprintf(" %d", int64(v))
					continue
				}
				formattedValue = fmt.Sprintf("%v", v)
			default:
				formattedValue = fmt.Sprintf("%v", v)
			}

			if isFirstField {
				fields += k + "=" + formattedValue
				isFirstField = false
			} else {
				fields += "," + k + "=" + formattedValue
			}
		}

		// If no timestamp, use current time
		if timestamp == "" {
			timestamp = fmt.Sprintf(" %d", time.Now().UnixNano()/int64(time.Millisecond))
		}

		// Create line protocol
		line := d.influxMeasurement
		if tags != "" {
			line += tags
		}
		line += " " + fields + timestamp

		lines = append(lines, line)
	}

	// Send data to InfluxDB
	payload := []byte(strings.Join(lines, "\n"))

	// Create URL with parameters
	apiUrl, err := url.Parse(d.influxUrl)
	if err != nil {
		return err
	}

	apiUrl.Path = "/api/v2/write"
	q := apiUrl.Query()
	q.Set("org", d.influxOrganization)
	q.Set("bucket", d.influxBucket)
	q.Set("precision", d.influxPrecision)
	apiUrl.RawQuery = q.Encode()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < d.maxRetries; i++ {
			req, err := http.NewRequest("POST", apiUrl.String(), bytes.NewBuffer(payload))
			if err != nil {
				if d.influxLogErrors {
					log.Errorf("Error creating request: %v", err)
				}
				return
			}

			req.Header.Set("Content-Type", "text/plain; charset=utf-8")
			req.Header.Set("Authorization", "Token "+d.influxToken)
			if d.influxGZip {
				req.Header.Set("Content-Encoding", "gzip")
			}

			client := &http.Client{
				Timeout: 10 * time.Second,
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: d.influxTLSSkipVerify,
					},
				},
			}

			resp, err := client.Do(req)
			if err != nil {
				if d.influxLogErrors {
					log.Errorf("Error sending data to InfluxDB: %v", err)
				}
				if i == d.maxRetries-1 {
					return
				}
				time.Sleep(d.retryDelay * time.Duration(math.Pow(2, float64(i)))) // exponential backoff
				continue
			}

			defer resp.Body.Close()

			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				if d.influxLogErrors {
					log.Errorf("InfluxDB responded with status code %d", resp.StatusCode)
				}
				if i == d.maxRetries-1 {
					return
				}
				time.Sleep(d.retryDelay * time.Duration(math.Pow(2, float64(i)))) // exponential backoff
				continue
			}

			// Success
			break
		}
	}()

	wg.Wait()
	return nil
}

func (d *InfluxDbDriver) Close() error {
	// Send final batch
	err := d.sendBatch()
	close(d.q)
	return err
}

func init() {
	d := &InfluxDbDriver{}
	transport.RegisterTransportDriver("influxdb", d)
}
