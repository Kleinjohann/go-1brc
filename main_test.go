package main

import (
    "io/fs"
    "log"
    "os"
    "path/filepath"
    "strings"
    "testing"
)

func Test_run(t *testing.T) {
    var basename string
    var result string
    var correct_result string
    input_extension := ".txt"
    output_extension := ".out"
    testFiles := find_test_files("test/", ".txt")
    for _, file := range testFiles {
        basename = strings.TrimSuffix(file, input_extension)
        result = run(file)
        correct_result = read_file(basename + output_extension)
        if result != correct_result {
            t.Errorf("Test Case %s:\nGot %s\nExpected %s\n", basename, result, correct_result)
        }
    }
}

func find_test_files(folder string, extension string) []string {
    var output []string
    filepath.WalkDir(folder, func(path string, d fs.DirEntry, err error) error {
        if err != nil {
            return err
        }
        if filepath.Ext(path) == extension {
            output = append(output, path)
        }
        return nil
    })
    return output
}

func read_file(file string) string {

    contents, err := os.ReadFile(file)

    if err != nil {
        log.Fatal(err)
    }

    return string(contents)
}
