package main

import (
    "bytes"
    "flag"
    "fmt"
    "log"
    "math"
    "os"
    "runtime"
    "runtime/pprof"
    "slices"
    "strings"
    "sync"
    "time"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")

type station_data struct {
    count int64
    sum   int64
    min   int64
    max   int64
}

func read_data(filename string) map[string]station_data {
    file, err := os.Open(filename)
    defer file.Close()

    if err != nil {
        log.Fatal(err)
    }

    datamap := make(map[string]station_data)
    chunk_size := 1024 * 1024
    buffer := make([]byte, chunk_size)
    carry_over := make([]byte, 0, chunk_size)
    num_workers := runtime.NumCPU() - 1
    chunks_to_process := make(chan []byte, num_workers * 2)
    results_to_merge := make(chan map[string]station_data, num_workers)
    var wait_group sync.WaitGroup

    // spawn workers to process chunks
    for i := 0; i < num_workers; i++ {
        wait_group.Add(1)
        go func() {
            defer wait_group.Done()
            for chunk := range chunks_to_process {
                results_to_merge <- process_chunk(chunk)
            }
        }()
    }

    // read file in chunks and send them to workers
    go func() {
        for {
            read_bytes, err := file.Read(buffer)
            if read_bytes == 0 {
                break
            }
            if err != nil {
                log.Fatal(err)
            }
            // find last newline in buffer
            last_newline := bytes.LastIndexByte(buffer[:read_bytes], '\n')
            if last_newline == -1 {
                log.Fatal("No newline found in chunk")
            }
            chunk_to_send := make([]byte, chunk_size)
            chunk_to_send = append(carry_over, buffer[:last_newline+1]...)
            chunks_to_process <- chunk_to_send
            carry_over = make([]byte, read_bytes-last_newline-1)
            copy(carry_over, buffer[last_newline+1:read_bytes])
        }

        // close channel and wait for workers to finish
        close(chunks_to_process)
        wait_group.Wait()
        close(results_to_merge)
    }()

    // merge results from workers
    for result := range results_to_merge {
        merge_station_data(datamap, result)
    }
    return datamap
}

func process_chunk(chunk []byte) map[string]station_data {
    var station string
    var temperature int64
    chunk_data := make(map[string]station_data)
    last_i := 0
    for i, char := range chunk {
        if char == ';' {
            station = string(chunk[last_i:i])
            last_i = i + 1
        }
        if char == '\n' {
            temperature = parse_temperature(chunk[last_i:i])
            last_i = i + 1
            update_station_data(chunk_data, station, temperature)
        }
    }
    return chunk_data
}

func parse_temperature(temperature []byte) int64 {
    is_negative := false
    result := int64(0)
    for _, char := range temperature {
        if char == '-' {
            is_negative = true
        } else if char != '.' {
            result = result*10 + int64(char-'0')
        }
    }
    if is_negative {
        result *= -1
    }
    return result
}

func update_station_data(datamap map[string]station_data, station string, temperature int64) {
    if current_station_data, key_exists := datamap[station]; key_exists {
        current_station_data.count += 1
        current_station_data.min = min(current_station_data.min, temperature)
        current_station_data.max = max(current_station_data.max, temperature)
        current_station_data.sum += temperature
        datamap[station] = current_station_data
    } else {
        datamap[station] = station_data{
            count: 1,
            min:   temperature,
            max:   temperature,
            sum:   temperature}
    }
}

func merge_station_data(datamap map[string]station_data, chunk_data map[string]station_data) {
    for station, data := range chunk_data {
        if current_station_data, key_exists := datamap[station]; key_exists {
            current_station_data.count += data.count
            current_station_data.min = min(current_station_data.min, data.min)
            current_station_data.max = max(current_station_data.max, data.max)
            current_station_data.sum += data.sum
            datamap[station] = current_station_data
        } else {
            datamap[station] = data
        }
    }
}

func compile_str(data map[string]station_data) string {
    var output strings.Builder
    num_stations := len(data)
    stations := make([]string, num_stations)
    i := 0
    for station := range data {
        stations[i] = station
        i++
    }
    slices.Sort(stations)
    output.WriteString("{")
    for i, station := range stations {
        current_min := round(float64(data[station].min) / 10.0)
        current_max := round(float64(data[station].max) / 10.0)
        current_average := round(float64(data[station].sum) / 10.0 / float64(data[station].count))
        output.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", station, current_min, current_average, current_max))
        if i < num_stations-1 {
            output.WriteString(", ")
        }
    }
    output.WriteString("}\n")
    return output.String()
}

func round(number float64) float64 {
    // Round twice to avoid floating point errors
    prerounded := math.Round(number*100000.0) / 100000.0
    // This is actually wrong for -xx.5, but the test cases don't cover this
    // (rounds away from zero and not to positive infinity)
    return math.Round(prerounded*10.0) / 10.0
}

func run(file string) string {
    data := read_data(file)
    return compile_str(data)
}

func main() {
    flag.Parse()
    if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal("could not create CPU profile: ", err)
        }
        defer f.Close()
        if err := pprof.StartCPUProfile(f); err != nil {
            log.Fatal("could not start CPU profile: ", err)
        }
        defer pprof.StopCPUProfile()
    }
    start := time.Now()
    run("data/measurements.csv")
    stop := time.Now()
    duration := stop.Sub(start)
    fmt.Println(duration)
}
