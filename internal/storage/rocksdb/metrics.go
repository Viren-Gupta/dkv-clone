package rocksdb

import (
	"log"
	"time"

	"github.com/flipkart-incubator/gorocksdb"
	"github.com/prometheus/client_golang/prometheus"
)

// metricsCollector collects rocksdB metrics.
func (rdb *rocksDB) metricsCollector() {

	memTableTotalGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "rocksdb",
			Name:      "memory_usage_memtable_total",
			Help:      "Rocksdb MemTableTotal estimates memory usage of all mem-tables",
		},
	)

	memTableUnflushedGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "rocksdb",
			Name:      "memory_usage_memtable_unflushed",
			Help:      "Rocksdb MemTableUnflushed estimates memory usage of unflushed mem-tables",
		},
	)

	memTableReadersTotalGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "rocksdb",
			Name:      "memory_usage_memtable_readers_total",
			Help:      "Rocksdb MemTableReadersTotal memory usage of table readers (indexes and bloom filters)",
		},
	)

	cacheTotalGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "rocksdb",
			Name:      "memory_usage_cache_total",
			Help:      "Rocksdb CacheTotal memory usage of cache",
		},
	)

	rdb.opts.promRegistry.MustRegister(cacheTotalGauge, memTableTotalGauge, memTableUnflushedGauge, memTableReadersTotalGauge)
	ticker := time.NewTicker(60 * time.Second)

	for {
		select {
		case <-rdb.closed:
			return
		case <-ticker.C:
			memoryUsage, err := gorocksdb.GetApproximateMemoryUsageByType([]*gorocksdb.DB{rdb.db}, nil)
			if err != nil {
				log.Printf("Error getting rocksdb memory usage: %v \n", err)
			} else {
				cacheTotalGauge.Set(float64(memoryUsage.CacheTotal))
				memTableTotalGauge.Set(float64(memoryUsage.MemTableTotal))
				memTableUnflushedGauge.Set(float64(memoryUsage.MemTableUnflushed))
				memTableReadersTotalGauge.Set(float64(memoryUsage.MemTableReadersTotal))
			}
		}
	}

}
