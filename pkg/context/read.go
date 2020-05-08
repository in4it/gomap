package context

func (d *Context) Read(filename string) *Context {
	d.input = filename
	return d
}

func (d *Context) ReadParquet(filename string, schema interface{}) *Context {
	d.input = filename
	d.inputSchema = schema
	d.inputType = "parquet"
	return d
}
