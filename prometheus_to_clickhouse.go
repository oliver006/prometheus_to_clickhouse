package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/kshvakov/clickhouse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

var (
	receivedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: ".",
		},
	)
	sentTransactionsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "clickhouse_sent_transactions_duration_seconds",
			Help:    ".",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"remote", "result"},
	)

	clickhouseAddr = flag.String("clickhouse-address", "tcp://127.0.0.1:9000", "Address of your clickhouse server")
	listenAddr     = flag.String("listen-addr", "0.0.0.0:9119", "Listen address for the adapter process")
)

func init() {
	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(sentTransactionsDuration)
}

func main() {
	connect, err := sql.Open("clickhouse", fmt.Sprintf("%s?debug=true", *clickhouseAddr))
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Fatalf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			log.Fatal(err)
		}
		return
	}

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {

		/*
		   This is code needs to be fixed up once Prometheus 1.7 is released as the
		   encoding changed, see: https://github.com/prometheus/prometheus/blob/8f781e411c79b6ef616dff3e074191da88ea0fde/documentation/examples/remote_storage/remote_storage_adapter/main.go#L187-L197
		*/
		reqBuf, err := ioutil.ReadAll(snappy.NewReader(r.Body))
		if err != nil {
			log.Printf("decode err: %s", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req remote.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		tx, _ := connect.Begin()
		stmt, _ := tx.Prepare("INSERT INTO metrics (EventDate, EventDateTime, Metric, Labels.Name, Labels.Value, Value) VALUES (?, ?, ?, ?, ?, ?)")
		txStart := time.Now().UnixNano()

		for _, ts := range req.Timeseries {
			var metric string

			lblNames := []string{}
			lblValues := []string{}
			for _, l := range ts.Labels {
				if model.LabelName(l.Name) == model.MetricNameLabel {
					metric = l.Value
				} else {
					lblNames = append(lblNames, l.Name)
					lblValues = append(lblValues, l.Value)
				}
			}

			receivedSamples.Add(float64(len(ts.Samples)))
			for _, s := range ts.Samples {

				eventDate := s.TimestampMs / 1000
				value := s.Value

				if _, err := stmt.Exec(
					time.Unix(eventDate, 0),
					time.Unix(eventDate, 0),
					metric,
					clickhouse.Array(lblNames),
					clickhouse.Array(lblValues),
					value,
				); err != nil {
					log.Fatal(err)
				}
			}
		}

		res := "ok"
		if err := tx.Commit(); err != nil {
			log.Errorf("Clickhouse transaction failed, err: %s", err)
			res = "failed"
		}

		took := (float64(time.Now().UnixNano()-txStart) / 1000000000.0)
		sentTransactionsDuration.WithLabelValues(*clickhouseAddr, res).Observe(took)
	})

	log.Printf("Listening at: %s", *listenAddr)
	http.Handle("/metrics", prometheus.Handler())
	http.ListenAndServe(*listenAddr, nil)
}
