package ui

import (
	"fmt"
	"strings"
	"time"
)

func (ui *UI) banner(f string, v ...interface{}) {
	text := fmt.Sprintf(
		"██████  DOGMA BANK • %s  ██",
		fmt.Sprintf(f, v...),
	)

	if n := 100 - len(text); n > 0 {
		text += strings.Repeat("█", n)
	}

	text += fmt.Sprintf(
		"  %s  ████████▓▓▓▒▒░",
		time.Now().Format("3:04:05 PM"),
	)

	ui.println(strings.ToUpper(text))
	ui.println("")

	if !ui.flushLog() {
		ui.println("")
	}
}

func (ui *UI) print(f string, v ...interface{}) {
	fmt.Printf(f, v...)
}

func (ui *UI) println(f string, v ...interface{}) {
	fmt.Printf(f+"\n", v...)
}

func (ui *UI) appendLog(s string) {
	ui.m.Lock()
	defer ui.m.Unlock()

	ui.log = append(
		ui.log,
		fmt.Sprintf(
			"%s   %s",
			time.Now().Format("3:04:05 PM"),
			s,
		),
	)
}

func (ui *UI) flushLog() bool {
	ui.m.Lock()
	log := ui.log
	ui.log = nil
	ui.m.Unlock()

	if len(log) == 0 {
		return false
	}

	ui.println("")
	ui.println("   ╭───┤ ENGINE LOG ├────────────────────────────────────────── ─── ── ─")
	ui.println("   │")

	for _, m := range log {
		ui.println("   │  %s", m)
	}

	ui.println("   │")
	ui.println("   ╰──────────────────────────────────────────── ─── ── ─")
	ui.println("")

	return true
}
