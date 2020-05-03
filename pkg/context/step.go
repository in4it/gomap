package context

func (d *Context) AddStep(s Step) {
	d.steps = append(d.steps, s)
}
