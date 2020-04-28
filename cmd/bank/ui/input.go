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

	if n != 0 {
		ui.print("%2d > ", n)
	} else if ui.ctx.Err() != nil {
		ui.print(" ✖ > ")
	} else {
		ui.print("   > ")
	}

	return len(ui.log) > 0
}

func (ui *UI) askText(prompt string) (string, bool) {
	ui.println("   %s (enter = cancel)", prompt)

	v := ui.askTextRaw()
	ui.println("")

	return v, v != ""
}

func (ui *UI) askTextWithDefault(prompt, def string) string {
	ui.println("   %s (enter = %#v)", prompt, def)

	v := ui.askTextRaw()
	ui.println("")

	if v == "" {
		return def
	}

	return v
}

func (ui *UI) askTextRaw() string {
	for {
		ui.printPrompt()
		v := ui.read()

		if v == "" {
			return ""
		}

		if v[0] != '/' {
			return v
		}

		switch v {
		case "/quit":
			ui.quit()
		case "/log":
			ui.flushLog()
		case "/help":
			ui.println("")
			ui.println("   /log   display infix log messages")
			ui.println("   /quit  exit immediately")
			ui.println("   /help  display a help message")
			ui.println("")
		default:
			ui.println("     unrecognized command, try /help")
			ui.println("")
		}
	}
}

type item struct {
	key  interface{}
	desc string
	next state
}

func (ui *UI) askMenu(
	fn func() ([]item, error),
) (state, error) {
	showOptions := true
	var options string

	for {
		items, err := fn()
		if err != nil {
			return nil, err
		}

		prev := options
		options = ""

		states := map[string]state{}
		for _, it := range items {
			k := fmt.Sprintf("%s", it.key)
			states[k] = it.next
			options += fmt.Sprintf("   %s) %s\n", k, it.desc)
		}

		if showOptions || prev != options {
			ui.println(" Select an option …")
			ui.println("")
			ui.println(options)
		}

		for {
			v := ui.askTextRaw()

			if v == "" {
				if ui.flushLog() {
					showOptions = true
					break
				}

				continue
			}

			if s, ok := states[v]; ok {
				ui.println("")
				return s, nil
			}

			if v == "?" {
				ui.println("")
				ui.println(options)
			} else {
				ui.println("     unrecognized option, try ? and /help")
				ui.println("")
			}
		}
	}
}
