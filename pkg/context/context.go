package context

func New() *Context {
	return &Context{}
}

func (c *Context) GetError() error {
	return c.err
}
