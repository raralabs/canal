package content

func Builder(contents ...IContent) IContent {
	if len(contents) > 1 {
		panic("Can't build a content from multiple contents.")
	}

	if len(contents) == 0 {
		return New()
	}
	// len(contents) is always 1 here
	content := contents[0]

	if content == nil {
		return New()
	}

	return content.Copy()
}
