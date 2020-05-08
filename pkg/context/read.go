package context

func (d *Context) Read(filename string) *Context {
	d.input = filename
	return d
}
