package ui

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var reader = bufio.NewReader(os.Stdin)

func (ui *UI) read() string {
	text, _ := reader.ReadString('\n')
	text = strings.TrimSpace(text)
	return text
}

func (ui *UI) printPrompt() bool {
	ui.m.Lock()
	n := len(ui.log)
	ui.m.Unlock()

	if n == 0 {
		ui.print("   > ")
	} else {
		ui.print("%2d > ", n)
	}

	return len(ui.log) > 0
}

func (ui *UI) askText(prompt string) (string, bool) {
	ui.println("   %s (enter = cancel)", prompt)
	ui.printPrompt()

	v := ui.read()
	ui.println("")

	return v, v != ""
}

func (ui *UI) askTextWithDefault(prompt, def string) string {
	ui.println("   %s (enter = %#v)", prompt, def)
	ui.printPrompt()

	v := ui.read()
	ui.println("")

	if v == "" {
		return def
	}

	return v
}

type item struct {
	key  interface{}
	desc string
	next state
}

func (ui *UI) askMenu(items ...item) (state, error) {
	items = append(items, item{"q", "quit", nil})
	states := map[string]state{}

	var options string

	for _, it := range items {
		k := fmt.Sprintf("%s", it.key)
		states[k] = it.next
		options += fmt.Sprintf("   %s) %s\n", k, it.desc)
	}

	for {
		ui.println(" Select an option …")
		ui.println("")
		ui.println(options)

		for {
			ui.printPrompt()

			v := ui.read()

			if v == "" {
				if ui.flushLog() {
					break
				}

				continue
			}

			if v == "?" {
				ui.println("")
				ui.println(options)
				continue
			}

			if s, ok := states[v]; ok {
				ui.println("")
				return s, nil
			}

			ui.println("     unrecognised option (%s), use ? to see the valid options", v)
		}
	}
}
