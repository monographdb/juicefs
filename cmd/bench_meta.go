/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

func cmdMetaBench() *cli.Command {
	selfFlags := []cli.Flag{
		&cli.UintFlag{
			Name:  "files",
			Value: 100,
			Usage: "files per thread",
		},
		&cli.UintFlag{
			Name:  "threads",
			Value: 1,
			Usage: "number of concurrent threads(goroutine)",
		},
		&cli.StringSliceFlag{
			Name:  "steps",
			Value: cli.NewStringSlice(stepNames...),
			Usage: "test suit steps",
		},
		&cli.StringFlag{
			Name:  "url",
			Usage: "metadata engine URL",
		},
		&cli.PathFlag{
			Name:  "cpuprofile",
			Usage: "write cpu profile to file",
		},
		&cli.PathFlag{
			Name:  "metric-out",
			Usage: "output metrics information",
		},
	}
	return &cli.Command{
		Name:        "mdbench",
		Action:      metadataBench,
		Category:    "TOOL",
		Usage:       "Run metadata benchmarks on a path",
		ArgsUsage:   "PATH",
		Description: "benchmark for metadata",
		Flags:       expandFlags(selfFlags, clientFlags(0), shareInfoFlags()),
	}
}

type StepKind uint8

const (
	stepCreate StepKind = iota
	stepStat
	stepOpen
	stepRemove
	stepNum
)

var stepNames = []string{stepCreate: "create", stepStat: "stat", stepOpen: "open", stepRemove: "remove"}

type Step struct {
	kind   StepKind
	repeat uint
}

type MetaBench struct {
	dir       string
	threads   uint
	files     uint
	funcs     [stepNum]func(string)
	pid       int
	purgeArgs []string
	jfs       *fs.FileSystem
}

func (b *MetaBench) prepare() {
	if b.jfs == nil {
		if _, err := os.Stat(b.dir); os.IsNotExist(err) {
			if err = os.MkdirAll(b.dir, os.ModePerm); err != nil {
				logger.Fatalf("Failed to create %s: %s", b.dir, err)
			}
		}
		for i := uint(0); i < b.threads; i++ {
			d := b.routine_dir(i)
			if _, err := os.Stat(d); os.IsNotExist(err) {
				if err = os.Mkdir(d, os.ModePerm); err != nil {
					logger.Fatalf("Failed to create %s: %s", d, err)
				}
			}
		}

		b.funcs[stepCreate] = func(fn string) {
			file, err := os.Create(fn)
			if err != nil {
				logger.Fatalf("Failed to create %s: %s", fn, err)
			}
			file.Close()
		}
		b.funcs[stepStat] = func(fn string) {
			_, err := os.Stat(fn)
			if err != nil {
				panic(err)
			}
		}
		b.funcs[stepOpen] = func(fn string) {
			file, err := os.Open(fn)
			if err != nil {
				panic(err)
			}
			file.Close()
		}
		b.funcs[stepRemove] = func(fn string) {
			err := os.Remove(fn)
			if err != nil {
				panic(err)
			}
		}
	} else {
		if _, err := b.jfs.Stat(ctx, b.dir); os.IsNotExist(err) {
			if err = b.jfs.MkdirAll(ctx, b.dir, 0777, umask); err != 0 {
				logger.Fatalf("Failed to create %s: %s", b.dir, err)
			}
		}
		for i := uint(0); i < b.threads; i++ {
			d := b.routine_dir(i)
			if _, err := b.jfs.Stat(ctx, d); os.IsNotExist(err) {
				if err = b.jfs.Mkdir(ctx, d, 0777, umask); err != 0 {
					logger.Fatalf("Failed to create %s: %s", d, err)
				}
			}
		}

		b.funcs[stepCreate] = func(fn string) {
			file, err := b.jfs.Create(ctx, fn, 0666, umask)
			if err != 0 {
				logger.Fatalf("Failed to create %s: %s", fn, err)
			}
			file.Close(ctx)
		}
		b.funcs[stepStat] = func(fn string) {
			_, err := b.jfs.Stat(ctx, fn)
			if err != 0 {
				logger.Fatalf("Failed to stat %s: %s", fn, err)
			}
		}
		b.funcs[stepOpen] = func(fn string) {
			file, err := b.jfs.Open(ctx, fn, meta.MODE_MASK_R|meta.MODE_MASK_W)
			if err != 0 {
				logger.Fatalf("Failed to open %s: %s", fn, err)
			}
			file.Close(ctx)
		}
		b.funcs[stepRemove] = func(fn string) {
			err := b.jfs.Delete(ctx, fn)
			if err != 0 {
				logger.Fatalf("Failed to delete %s: %s", fn, err)
			}
		}
	}
}

func (b *MetaBench) run(step StepKind, repeat uint) {
	if len(b.purgeArgs) > 0 {
		b.dropCaches()
	}
	stepFunc := b.funcs[step]
	wg := sync.WaitGroup{}
	wg.Add(int(b.threads))
	start := time.Now()
	for tid := uint(0); tid < b.threads; tid++ {
		go func(i uint) {
			d := b.routine_dir(i)
			for r := uint(0); r < repeat; r++ {
				for fid := uint(0); fid < b.files; fid++ {
					fn := b.filename(d, fid)
					stepFunc(fn)
				}
			}
			wg.Done()
		}(tid)
	}
	wg.Wait()
	cost := time.Since(start)
	total := b.threads * b.files * repeat
	ops := float64(total) / cost.Seconds()
	logger.Infof("%s: %d operations, cost %v, OPS=%.2f", strings.ToUpper(stepNames[step]), total, cost, ops)
}

func (b *MetaBench) routine_dir(i uint) string {
	return filepath.Join(b.dir, fmt.Sprintf("meta-bench-%d", i))
}

func (b *MetaBench) filename(d string, i uint) string {
	return filepath.Join(d, fmt.Sprintf("file-%d", i))
}

func (b *MetaBench) dropCaches() {
	if err := exec.Command(b.purgeArgs[0], b.purgeArgs[1:]...).Run(); err != nil {
		logger.Warnf("Failed to clean kernel caches: %s", err)
	}
}

func (b *MetaBench) outputMetrics(ctx *cli.Context, idx int, step string) {
	url := ctx.String("metrics")
	out := ctx.Path("metric-out")
	if url == "" || out == "" {
		return
	}
	if err := os.MkdirAll(out, 0666); err != nil {
		log.Fatal(err)
	}
	res, err := http.Get("http://" + url + "/metrics")
	if err != nil {
		log.Fatal(err)
	}
	filename := fmt.Sprintf("%d-%d-%s", b.pid, idx, step)
	file, err := os.Create(out + filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(res.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "juicefs_meta_ops") {
			continue
		}
		_, err = file.WriteString(line)
		if err != nil {
			log.Fatal(err)
		}
		_, err = file.WriteString("\n")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func metadataBench(ctx *cli.Context) error {
	setup(ctx, 1)
	mount_point, err := filepath.Abs(ctx.Args().First())
	if err != nil {
		logger.Fatalf("Failed to get absolute path of %s: %s", ctx.Args().First(), err)
	}
	threads := ctx.Uint("threads")
	files := ctx.Uint("files")
	if files == 0 || threads == 0 {
		return os.ErrInvalid
	}
	bench := MetaBench{
		dir:     mount_point,
		threads: threads,
		files:   files,
		pid:     os.Getpid(),
	}
	metaUrl := ctx.String("url")
	if metaUrl != "" {
		jfs := initForMdtest(ctx, "mdbench", metaUrl)
		bench.jfs = jfs
	} else {
		var purgeArgs []string
		if os.Getuid() != 0 {
			purgeArgs = append(purgeArgs, "sudo")
		}
		switch runtime.GOOS {
		case "darwin":
			purgeArgs = append(purgeArgs, "purge")
		case "linux":
			purgeArgs = append(purgeArgs, "/bin/sh", "-c", "echo 3 > /proc/sys/vm/drop_caches")
		default:
			logger.Fatal("Currently only support Linux/macOS")
		}
		if os.Getuid() != 0 {
			logger.Infof("Clean kernel cache may ask for root privilege...")
		}
		bench.purgeArgs = purgeArgs
	}
	bench.prepare()
	cpuprofile := ctx.Path("cpuprofile")
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	logger.Infof("metadata benchmark start...")
	stepsArg := ctx.StringSlice("steps")
	steps := make([]Step, 0, len(stepsArg))
	for _, step := range stepsArg {
		p := strings.IndexByte(step, '*')
		repeat := uint(1)
		if p > 0 {
			n, err := strconv.Atoi(step[p+1:])
			if err != nil {
				log.Fatal(err)
			}
			repeat = uint(n)
			step = step[:p]
		}
		var skind StepKind
		switch step {
		case "create", "c":
			skind = stepCreate
		case "stat", "s":
			skind = stepStat
		case "open", "o":
			skind = stepOpen
		case "remove", "rm", "r", "delete", "del", "d":
			skind = stepRemove
		default:
			log.Fatalf("unknown step '%s'", step)
		}
		steps = append(steps, Step{
			kind:   skind,
			repeat: repeat,
		})
	}
	for i, step := range steps {
		bench.run(step.kind, step.repeat)
		bench.outputMetrics(ctx, i, stepNames[step.kind])
	}
	return nil
}
