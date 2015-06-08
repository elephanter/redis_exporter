package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/elephanter/redis_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	redisAddr     = flag.String("redis.addr", "localhost:6379", "Address of one or more redis nodes, comma separated")
	namespace     = flag.String("namespace", "redis", "Namespace for metrics")
	listenAddress = flag.String("web.listen-address", ":9121", "Address to listen on for web interface and telemetry.")
	metricPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
)

func main() {
	flag.Parse()

	addrs := strings.Split(*redisAddr, ",")
	if len(addrs) == 0 || len(addrs[0]) == 0 {
		log.Fatal("Invalid parameter --redis.addr")
	}

	e := exporter.NewRedisExporter(addrs, *namespace)
	prometheus.MustRegister(e)

	http.Handle(*metricPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
						<head><title>Redis exporter</title></head>
						<body>
						<h1>Redis exporter</h1>
						<p><a href='` + *metricPath + `'>Metrics</a></p>
						</body>
						</html>
						`))
	})

	log.Printf("providing metrics at %s%s", *listenAddress, *metricPath)
	log.Printf("Connecting to: %#v", addrs)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
