package context

func (d *Context) read(filename string) *Context {
	d.input = filename
	return d
}
