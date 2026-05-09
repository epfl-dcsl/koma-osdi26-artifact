/*
 * MIT License
 *
 * Copyright (c) 2019-2021 Ecole Polytechnique Federale Lausanne (EPFL)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package main

import (
	"fmt"
)

func computeStatsThroughput(replies []*ThroughputReply) *ThroughputReply {
	agg_stats := &ThroughputReply{}
	for _, r := range replies {
		agg_stats.RxBytes += r.RxBytes
		agg_stats.TxBytes += r.TxBytes
		agg_stats.ReqCount += r.ReqCount
		agg_stats.CorrectIAD += r.CorrectIAD
	}
	agg_stats.Duration = replies[0].Duration

	return agg_stats
}

func computeStatsLatency(replies []*LatencyReply) *LatencyReply {
	agg_stats := &LatencyReply{}
	for _, r := range replies {
		printLatencyStats(r)
		agg_stats.AvgLat += r.AvgLat
		agg_stats.P50I += r.P50I
		agg_stats.P50 += r.P50
		agg_stats.P50K += r.P50K
		agg_stats.P90I += r.P90I
		agg_stats.P90 += r.P90
		agg_stats.P90K += r.P90K
		agg_stats.P95I += r.P95I
		agg_stats.P95 += r.P95
		agg_stats.P95K += r.P95K
		agg_stats.P99I += r.P99I
		agg_stats.P99 += r.P99
		agg_stats.P99K += r.P99K
		agg_stats.P999I += r.P999I
		agg_stats.P999 += r.P999
		agg_stats.P999K += r.P999K
		agg_stats.P9999I += r.P9999I
		agg_stats.P9999 += r.P9999
		agg_stats.P9999K += r.P9999K
		agg_stats.P99999I += r.P99999I
		agg_stats.P99999 += r.P99999
		agg_stats.P99999K += r.P99999K
		agg_stats.P999999I += r.P999999I
		agg_stats.P999999 += r.P999999
		agg_stats.P999999K += r.P999999K
		agg_stats.IsStationary += r.IsStationary
		agg_stats.IsIid += r.IsIid
	}
	// Calculate averages
	numReplies := uint64(len(replies))
	agg_stats.AvgLat /= numReplies
	agg_stats.P50I /= numReplies
	agg_stats.P50 /= numReplies
	agg_stats.P50K /= numReplies
	agg_stats.P90I /= numReplies
	agg_stats.P90 /= numReplies
	agg_stats.P90K /= numReplies
	agg_stats.P95I /= numReplies
	agg_stats.P95 /= numReplies
	agg_stats.P95K /= numReplies
	agg_stats.P99I /= numReplies
	agg_stats.P99 /= numReplies
	agg_stats.P99K /= numReplies
	agg_stats.P999I /= numReplies
	agg_stats.P999 /= numReplies
	agg_stats.P999K /= numReplies
	agg_stats.P9999I /= numReplies
	agg_stats.P9999 /= numReplies
	agg_stats.P9999K /= numReplies
	agg_stats.P99999I /= numReplies
	agg_stats.P99999 /= numReplies
	agg_stats.P99999K /= numReplies
	agg_stats.P999999I /= numReplies
	agg_stats.P999999 /= numReplies
	agg_stats.P999999K /= numReplies

	if agg_stats.IsIid == 0 {
		agg_stats.ToReduceSampling = 1000000
		for _, r := range replies {
			if r.ToReduceSampling < agg_stats.ToReduceSampling {
				agg_stats.ToReduceSampling = r.ToReduceSampling
			}
		}
	}
	return agg_stats

}

func printThroughputStats(stats *ThroughputReply) {
	fmt.Println("Next line includes both load and measurement")
	fmt.Println("#ReqCount\tQPS\tRxBw\tTxBw")
	fmt.Printf("%v\t%v\t%v\t%v\n", stats.ReqCount,
		1e6*float64(stats.ReqCount)/float64(stats.Duration),
		1e6*float64(stats.RxBytes)/float64(stats.Duration),
		1e6*float64(stats.TxBytes)/float64(stats.Duration))
}

func printLatencyStats(stats *LatencyReply) {
	fmt.Println("#Avg Lat\t50th\t90th\t95th\t99th\t99.9th\t99.99th\t99.999th\t99.9999th")
	fmt.Printf("%v\t%v(%v, %v)\t%v(%v, %v)\t%v(%v, %v)\t%v(%v, %v)\t%v(%v, %v)\t%v(%v, %v)\t%v(%v, %v)\t%v(%v, %v)\n",
		float64(stats.AvgLat)/1e3,
		float64(stats.P50)/1e3, float64(stats.P50I)/1e3, float64(stats.P50K)/1e3,
		float64(stats.P90)/1e3, float64(stats.P90I)/1e3, float64(stats.P90K)/1e3,
		float64(stats.P95)/1e3, float64(stats.P95I)/1e3, float64(stats.P95K)/1e3,
		float64(stats.P99)/1e3, float64(stats.P99I)/1e3, float64(stats.P99K)/1e3,
		float64(stats.P999)/1e3, float64(stats.P999I)/1e3, float64(stats.P999K)/1e3,
		float64(stats.P9999)/1e3, float64(stats.P9999I)/1e3, float64(stats.P9999K)/1e3,
		float64(stats.P99999)/1e3, float64(stats.P99999I)/1e3, float64(stats.P99999K)/1e3,
		float64(stats.P999999)/1e3, float64(stats.P999999I)/1e3, float64(stats.P999999K)/1e3)
}

func getRPS(stats *ThroughputReply) float64 {
	return 1e6 * float64(stats.ReqCount) / float64(stats.Duration)
}

func getLatCISize(stats []*LatencyReply, percentile int) int {
	if percentile == 99 {
		return int(stats[0].P99K - stats[0].P99I)
	} else {
		panic("Unknown percentile")
	}
}
