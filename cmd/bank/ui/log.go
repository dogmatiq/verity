package ui

import (
	"fmt"
)

// Log writes an application log message formatted according to a format
// specifier.
func (ui *UI) Log(f string, v ...interface{}) {
	ui.LogString(fmt.Sprintf(f, v...))
}

// LogString writes a pre-formatted application log message.
func (ui *UI) LogString(s string) {
	ui.appendLog("     " + s)
}

// Debug writes a debug log message formatted according to a format
// specifier.
func (ui *UI) Debug(f string, v ...interface{}) {
	ui.DebugString(fmt.Sprintf(f, v...))
}

// DebugString writes a pre-formatted debug log message.
func (ui *UI) DebugString(s string) {
	ui.appendLog("DBG  " + s)
}

// IsDebug returns true if this logger will perform debug logging.
func (ui *UI) IsDebug() bool {
	return true
}
