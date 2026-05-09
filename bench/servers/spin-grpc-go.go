//go:build spin

package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	// "google.golang.org/grpc/timetrace"
	"log"
	"math/rand"
	"net"
	"os"
	// "os/signal"
	"strconv"
	"strings"
	// "syscall"
	"time"

	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"

	proto "main/proto.com"
)

var (
	port = 40001
	tt   = false
)

// server implements the gRPC GreeterServer
type server struct {
	proto.UnimplementedEchoServer
}

// SendEcho implements echo.EchoServer
func (s *server) SendEcho(_ context.Context, in *proto.EchoRequest) (*proto.EchoResponse, error) {
	// log.Printf("Received: %v", in.GetMessage())
	processRequest() // call your service-time generator
	return &proto.EchoResponse{Message: "Echo: " + in.GetMessage()[:1024]}, nil
}

//
// ==== Service Time Generators ====
//

type Generator interface {
	Generate() int
}

// Fixed
type FixedGen struct{ val int }

func (g *FixedGen) Generate() int { return g.val }

// Exponential
type ExpGen struct{ lambda float64 }

func (g *ExpGen) Generate() int {
	return int(rand.ExpFloat64() / g.lambda)
}

// Bimodal
type BimodalGen struct {
	prob       float64
	val1, val2 float64
}

func (g *BimodalGen) Generate() int {
	if rand.Float64() < g.prob {
		return int(g.val1)
	}
	return int(g.val2)
}

var generator Generator

func resolveNumWorkers(defaultWorkers uint32) uint32 {
	cores := os.Getenv("GRPC_KOMA_CORES")
	if cores == "" {
		return defaultWorkers
	}

	var count uint32
	for _, core := range strings.Split(cores, ",") {
		if strings.TrimSpace(core) != "" {
			count++
		}
	}
	if count == 0 {
		return defaultWorkers
	}
	return count
}

func parseGenerator(spec string) Generator {
	parts := strings.Split(spec, ":")
	if len(parts) < 1 {
		log.Fatalf("invalid distribution spec: %s", spec)
	}
	name := parts[0]
	args := []string{}
	if len(parts) > 1 {
		args = strings.Split(parts[1], ",")
	}

	switch name {
	case "fixed":
		val, _ := strconv.Atoi(args[0])
		return &FixedGen{val}
	case "exponential":
		lambda, _ := strconv.ParseFloat(args[0], 64)
		return &ExpGen{lambda}
	case "bimodal":
		if len(args) != 3 {
			log.Fatalf("bimodal requires prob,val1,val2")
		}
		prob, _ := strconv.ParseFloat(args[0], 64)
		val1, _ := strconv.ParseFloat(args[1], 64)
		val2, _ := strconv.ParseFloat(args[2], 64)
		return &BimodalGen{prob: prob, val1: val1, val2: val2}
	default:
		log.Fatalf("unsupported distribution: %s", name)
	}
	return nil
}

//
// ==== Request processing ====
//

func spin(usecs int) {
	start := time.Now()
	for time.Since(start) < time.Duration(usecs)*time.Microsecond {
		// busy loop
	}
}

func processRequest() {
	svcTime := generator.Generate()
	spin(svcTime)
}

func help() {
	fmt.Printf("Usage: %s <distribution>\n\n", os.Args[0])
	fmt.Println("Distributions:")
	fmt.Println("   fixed:<value>                  Always <value> µs")
	fmt.Println("   exponential:<lambda>           Exponential with rate λ (mean=1/λ µs)")
	fmt.Println("   bimodal:<prob>,<val1>,<val2>   With probability <prob> choose <val1>, else <val2>")
	fmt.Println()
}

func main() {
	//  automatic GC globally.
	debug.SetGCPercent(-1)
	if len(os.Args) < 2 {
		help()
		os.Exit(1)
	}

	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	// e := os.Getenv("GODEBUG")
	// if strings.Contains(e, "tt") {
	// 	tt = true
	// 	// timetrace related
	// 	timetrace.Init()
	// 	log.Printf("TimeTrace enabled")
	// 	defer func() {
	// 		timetrace.Freeze()
	// 	}()
	// 	// Catch Ctrl-C / SIGTERM and stop server gracefully
	// 	stop := make(chan os.Signal, 1)
	// 	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	// 	go func() {
	// 		<-stop
	// 		fmt.Println("Stop() returned!") // <--- this line is key
	// 		timetrace.Freeze()
	// 		os.Exit(0)
	// 	}()
	// }

	distSpec := os.Args[1]
	rand.Seed(time.Now().UnixNano())
	generator = parseGenerator(distSpec)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	numWorkers := resolveNumWorkers(20)
	maxStreams := uint32(1000000) // maximum concurrent streams per connection
	s := grpc.NewServer(grpc.NumStreamWorkers(numWorkers), grpc.MaxConcurrentStreams(maxStreams), grpc.InitialConnWindowSize(126553500))
	proto.RegisterEchoServer(s, &server{})
	log.Printf("server listening at %v (dist=%s)", lis.Addr(), distSpec)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
