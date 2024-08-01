# One Billion Rows Challenge in golang
Trying this challenge to learn Go.

See the [official website](https://1brc.dev) for background information and instructions on how to generate the data.

Benchmarks are run on a Framework Laptop with an Intel Core i5-1340P, 32GB DDR4-3200 RAM and a WD_BLACK SN850X NVMe M.2 2280 SSD.

## Baseline: 2min43s

- read line by line
- use `strings.split` and `strconv.ParseFloat` for parsing
- use `float64` for temperatures
- no concurrency

## Custom Integer Parser: 1min40s

- custom parser to read station name and temperature
- use `int64` for temperatures
- abolish wrapper struct for parsed data

## Goroutines: 25s

- one routine to read file in chunks
- `NumCPU - 1` routines to process chunks
- split lines manually instead of using `Scanner.Scan`
