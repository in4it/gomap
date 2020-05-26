package context

func (c *Context) Read(filename string) *Context {
	c.input = filename
	c.inputType = "localfile"
	return c
}

func (c *Context) ReadParquet(filename string, schema interface{}) *Context {
	c.input = filename
	c.inputSchema = schema
	c.inputType = "parquet"
	return c
}
