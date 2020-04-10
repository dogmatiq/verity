package ui

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

func (ui *UI) banner(f string, v ...interface{}) {
	head := fmt.Sprintf(
		"██████  DOGMA BANK • %s  ██",
		fmt.Sprintf(f, v...),
	)
	tail := "██▓▓▓▒▒░"

	status := fmt.Sprintf(
		"  %s  ██",
		time.Now().Format("3:04:05 PM"),
	)

	if ui.ctx.Err() != nil {
		status += "  ✖ STOPPED  ██"
	}

	length := utf8.RuneCountInString(head + status + tail)
	var padding string
	if n := 118 - length; n > 0 {
		padding = strings.Repeat("█", n)
	}

	ui.print(head)
	ui.print(padding)
	ui.print(status)
	ui.print(tail)
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
			"%s  %s",
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
