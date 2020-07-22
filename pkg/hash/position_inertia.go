package hash

type PositionInertia byte

func (p *PositionInertia) Write(bss ...[]byte) {
	var blockHash byte
	var i int
	for _, bs := range bss {
		for _, b := range bs {
			blockHash ^= loopLeftMove(b, i)
			i += 1
		}
	}
	*p ^= PositionInertia(blockHash)
}

func loopLeftMove(source byte, step int) byte {
	step %= 8
	if step < 0 {
		step += 8
	}
	return source>>(8-step) | (source << step)
}
