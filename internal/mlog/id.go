package mlog

// FormatID formats a message ID for logging.
//
// If the message ID appears to be a UUID, only the first 8 characters are
// shown. Otherwise, the ID is displayed in-full.
func FormatID(id string) string {
	if len(id) == 36 && id[8] == '-' {
		return id[:8]
	}

	return id
}
